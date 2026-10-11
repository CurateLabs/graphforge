//! A strict ontology refuses an initial import whose types it does not declare,
//! through the public `validate` (#1881 intake refusals; ADR 0058).
//!
//! Finding recorded with this test: under a strict single ontology an initial
//! import is refused before any row is read, with "construction semantic
//! bindings are absent", whatever its types are. The storage layer's owner check
//! (`unknown strict ontology entity type`, exercised below the facade by
//! `the_bulk_reader_refuses_undeclared_types_under_a_strict_ontology`) is
//! therefore not reachable through the public route on an empty project. This
//! test pins what is observable there: the import is refused by `validate`, no
//! rows are accepted, nothing is published, and the session cannot commit.

use std::fs;

use graphforge_api::{
    AdoptOntologyRequest, BulkInputKind, GraphForge, OntologyMode, OperationId, WriteContext,
};
use uuid::Uuid;

use super::support::*;

const STRICT_ONTOLOGY: &str = "ontology_id: bulk\nversion: \"1\"\nentity_types:\n  - name: Host\n    abstract: false\nrelation_types:\n  - name: CONNECTS\n    src: Host\n    dst: Host\nproperties: []\n";

#[test]
fn a_strict_ontology_refuses_an_import_of_undeclared_types_at_validate() {
    let directory = tempfile::tempdir().unwrap();
    // Nodes labelled `Person`, which the ontology does not declare.
    let sources = Sources::write(&directory.path().join("input"), Spec::graph(10, 10));
    let project = empty_project(directory.path());
    let ontology = directory.path().join("strict.yaml");
    fs::write(&ontology, STRICT_ONTOLOGY).unwrap();
    let mut graph = GraphForge::new(project.to_str()).unwrap();
    graph
        .adopt_ontology(AdoptOntologyRequest {
            context: WriteContext {
                operation_uuid: OperationId(v7(7)),
                actor_uuid: None,
            },
            path: ontology,
            mode: OntologyMode::Strict,
        })
        .unwrap();
    let before = current_generation(&project);

    let mut session = graph
        .begin_import_session(OperationId(Uuid::now_v7()), limits())
        .unwrap();
    session
        .register_parquet(BulkInputKind::Node, &sources.nodes())
        .unwrap();
    session
        .register_parquet(BulkInputKind::Edge, &sources.edges())
        .unwrap();
    let refusal = session.validate(&graph).unwrap_err().to_string();
    assert!(
        refusal.contains("semantic bindings are absent")
            || refusal.contains("unknown strict ontology entity type"),
        "{refusal}"
    );
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
