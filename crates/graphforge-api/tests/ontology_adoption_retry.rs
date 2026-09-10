//! Public adoption replay must preserve facade and selected-generation authority.

use arrow::array::FixedSizeBinaryArray;
use graphforge_api::{AdoptOntologyRequest, GraphForge, OntologyMode, OperationId, WriteContext};
use std::path::Path;
use uuid::Uuid;

fn request(root: &Path) -> AdoptOntologyRequest {
    let path = root.join("promotion.yaml");
    std::fs::write(
        &path,
        "ontology_id: retry\nversion: \"1\"\nentity_types:\n  - name: Person\n    abstract: false\nrelation_types: []\n",
    )
    .unwrap();
    AdoptOntologyRequest {
        context: WriteContext {
            operation_uuid: OperationId(Uuid::from_u128(1_229_801)),
            actor_uuid: None,
        },
        path,
        mode: OntologyMode::Advisory,
    }
}

fn selected(root: &Path) -> Uuid {
    graphforge_storage::resolve_project_generation(root)
        .unwrap()
        .generation_uuid()
}

fn people(graph: &GraphForge) -> Vec<Uuid> {
    let result = graph
        .execute("MATCH (n:Person) RETURN n.node_uuid")
        .unwrap();
    let mut ids = Vec::new();
    for batch in result.batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            ids.push(Uuid::from_slice(values.value(row)).unwrap());
        }
    }
    ids.sort_unstable();
    ids
}

#[test]
fn exact_replay_on_a_stale_handle_cannot_succeed_with_exploratory_authority() {
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("project");
    let graph = GraphForge::new(project.to_str()).unwrap();
    graph.execute("CREATE (:Person), (:Person)").unwrap();
    drop(graph);
    let mut first = GraphForge::new(project.to_str()).unwrap();
    let mut stale = GraphForge::new(project.to_str()).unwrap();
    let expected = people(&first);
    assert_eq!(expected.len(), 2);
    let adoption = request(root.path());
    first.adopt_ontology(adoption.clone()).unwrap();
    let published = selected(&project);

    let replay = stale.adopt_ontology(adoption.clone());
    assert_eq!(selected(&project), published);
    if replay.is_ok() {
        assert_eq!(stale.ontology_mode(), OntologyMode::Advisory);
        assert_eq!(people(&stale), expected);
    }

    // An explicit stale-handle refusal is safe; reopening must make the exact
    // same authored request succeed without publishing another generation.
    let mut reopened = GraphForge::new(project.to_str()).unwrap();
    reopened.adopt_ontology(adoption).unwrap();
    assert_eq!(reopened.ontology_mode(), OntologyMode::Advisory);
    assert_eq!(people(&reopened), expected);
    assert_eq!(selected(&project), published);
}

#[test]
fn exact_replay_after_subsequent_mutation_never_rewinds_current() {
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("project");
    let mut graph = GraphForge::new(project.to_str()).unwrap();
    graph.execute("CREATE (:Person)").unwrap();
    let adoption = request(root.path());
    graph.adopt_ontology(adoption.clone()).unwrap();
    let adopted = selected(&project);
    let mut stale = GraphForge::new(project.to_str()).unwrap();
    graph.execute("CREATE (:Person)").unwrap();
    let newer = selected(&project);
    assert_ne!(newer, adopted);
    let expected = people(&graph);
    assert_eq!(expected.len(), 2);

    let stale_replay = stale.adopt_ontology(adoption.clone());
    assert_eq!(selected(&project), newer);
    if stale_replay.is_ok() {
        assert_eq!(people(&stale), expected);
    }

    graph.adopt_ontology(adoption.clone()).unwrap();
    assert_eq!(selected(&project), newer);
    assert_eq!(people(&graph), expected);
    let mut reopened = GraphForge::new(project.to_str()).unwrap();
    reopened.adopt_ontology(adoption).unwrap();
    assert_eq!(selected(&project), newer);
    assert_eq!(people(&reopened), expected);
}

#[test]
fn typed_readoption_refuses_changed_identity_assignment_before_current() {
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("project");
    let mut graph = GraphForge::new(project.to_str()).unwrap();
    let mut adoption = request(root.path());
    std::fs::write(&adoption.path, "ontology_id: retry\nversion: \"1\"\nentity_types:\n  - name: Person\n    abstract: false\n  - name: Ghost\n    abstract: false\nrelation_types: []\n").unwrap();
    graph.adopt_ontology(adoption.clone()).unwrap();
    graph.execute("CREATE (:Person), (:Person)").unwrap();
    let expected = people(&graph);
    assert_eq!(expected.len(), 2);
    let before = selected(&project);

    for (operation, declarations) in [
        (
            1_229_802,
            "  - name: Ghost\n    abstract: false\n  - name: Person\n    abstract: false\n",
        ),
        (1_229_803, "  - name: Ghost\n    abstract: false\n"),
    ] {
        adoption.context.operation_uuid = OperationId(Uuid::from_u128(operation));
        std::fs::write(&adoption.path, format!("ontology_id: retry\nversion: \"2\"\nentity_types:\n{declarations}relation_types: []\n")).unwrap();
        assert!(graph.adopt_ontology(adoption.clone()).is_err());
        assert_eq!(selected(&project), before);
        assert_eq!(graph.ontology_mode(), OntologyMode::Advisory);
        assert_eq!(people(&graph), expected);
        let reopened = GraphForge::new(project.to_str()).unwrap();
        assert_eq!(people(&reopened), expected);
    }
}
