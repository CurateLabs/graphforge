//! A facade that survives `revert_to_checkpoint` must keep writing, and neither
//! the checkpoint's generation tree nor the restored generation's tree may be
//! written in place (#1709).
use super::{CheckpointRequest, RevertCheckpointRequest};
use crate::expanded_generation_test_support::into_expanded;
use crate::pinned_workspace_tests::{
    TreeStamp, assert_published_trees_untouched, published_generations, tree_drift,
};
use crate::{GraphForge, OperationId};
use arrow::array::{Array, StringArray};
use uuid::Uuid;

fn operation(value: u128) -> OperationId {
    OperationId(Uuid::from_u128(value))
}

fn names(graph: &GraphForge) -> Vec<String> {
    let rows = graph
        .execute("MATCH (n:Person) RETURN n.name AS name ORDER BY name")
        .unwrap();
    let column = rows.batches[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..column.len())
        .map(|row| column.value(row).to_owned())
        .collect()
}

#[test]
fn write_after_revert_succeeds_and_modifies_no_published_tree() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (:Person {name:'before'})").unwrap();
    // The hazard under test is a pinned alias of a generation tree, which only
    // an expanded generation has; a compact root hydrates privately.
    let mut graph = into_expanded(graph);
    graph
        .checkpoint(CheckpointRequest {
            name: "cp".into(),
            description: None,
            idempotency_key: operation(1),
            actor_uuid: None,
        })
        .unwrap();
    graph.execute("CREATE (:Person {name:'after'})").unwrap();
    graph
        .revert_to_checkpoint(RevertCheckpointRequest {
            name: "cp".into(),
            reason: "return to the checkpoint".into(),
            idempotency_key: operation(2),
            actor_uuid: None,
        })
        .unwrap();
    assert_eq!(names(&graph), ["before"]);
    // Every generation published so far: the checkpoint's, the intermediate
    // one, and the restored current generation.
    let before = published_generations(&root);
    let restored = graph.generation_for_read().unwrap();
    for (generation, _) in &before {
        assert_ne!(
            graph.dir().path(),
            generation.graph_tree_root(),
            "the live facade must not alias published generation {}",
            generation.generation_uuid()
        );
    }

    graph
        .execute("CREATE (:Person {name:'after-revert'})")
        .expect("a revert must leave the facade writable");

    assert_eq!(names(&graph), ["after-revert", "before"]);
    assert_ne!(
        graph.generation_for_read().unwrap().generation_uuid(),
        restored.generation_uuid(),
        "the write publishes a new generation"
    );
    assert_published_trees_untouched(&before, "a write after revert_to_checkpoint");
}

#[test]
fn checkpoint_view_trees_survive_a_revert_then_write_cycle_twice() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (:Person {name:'before'})").unwrap();
    // The hazard under test is a pinned alias of a generation tree, which only
    // an expanded generation has; a compact root hydrates privately.
    let mut graph = into_expanded(graph);
    graph
        .checkpoint(CheckpointRequest {
            name: "cp".into(),
            description: None,
            idempotency_key: operation(10),
            actor_uuid: None,
        })
        .unwrap();
    let (_, source) = graphforge_storage::open_checkpoint_generation_with_mode(
        &root,
        "cp",
        graphforge_storage::filesystem_admission::ProjectLifecycleMode::Durable,
    )
    .unwrap();
    let stamp = TreeStamp::capture(&source.graph_tree_root());

    for round in 0..2u128 {
        graph
            .execute(&format!("CREATE (:Person {{name:'round-{round}'}})"))
            .unwrap();
        graph
            .revert_to_checkpoint(RevertCheckpointRequest {
                name: "cp".into(),
                reason: "again".into(),
                idempotency_key: operation(20 + round),
                actor_uuid: None,
            })
            .unwrap();
        graph
            .execute(&format!("CREATE (:Person {{name:'kept-{round}'}})"))
            .unwrap();
        assert_eq!(
            names(&graph),
            ["before".to_owned(), format!("kept-{round}")],
            "round {round}"
        );
        assert_eq!(
            tree_drift(&source, &stamp),
            Vec::<String>::new(),
            "round {round} modified the checkpoint generation's tree"
        );
    }
}
