//! OPTIONAL MATCH uses the public query memory policy for materialization.
use arrow::array::{Array, StringArray};
use graphforge_api::{ExecutionResourcePolicy, ExplainStage, GraphForge, GraphForgeOptions};

fn graph(budget: u64) -> GraphForge {
    let graph = GraphForge::new_with_options(
        None,
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                memory_budget_bytes: Some(budget),
                target_partitions: Some(1),
                compute_threads: Some(1),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .unwrap();
    let payload = "x".repeat(1024);
    graph
        .execute(&format!(
            "UNWIND range(1, 100) AS id CREATE (:Item {{id:id, payload:'{payload}'}})"
        ))
        .unwrap();
    graph
}

#[test]
fn optional_match_wide_cartesian_refuses_typed_and_fitting_budget_preserves_every_cell() {
    const QUERY: &str = "MATCH (a:Item) OPTIONAL MATCH (b:Item) RETURN a.payload, b.payload";
    let tight = graph(16 * 1024 * 1024);
    let plan = tight
        .explain_stage(QUERY, ExplainStage::PhysicalPlan)
        .unwrap();
    assert!(plan.contains("OptionalMatchExec"), "{plan}");
    let error = tight
        .execute(QUERY)
        .err()
        .expect("wide optional output must exceed the small pool");
    let text = error.to_string();
    assert!(
        text.contains("OptionalMatch") && text.contains("Resources exhausted"),
        "{text}"
    );
    // A failed optional does not poison the project or retain its query pool.
    assert_eq!(
        tight
            .execute("MATCH (a:Item) RETURN count(a)")
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    let roomy = graph(128 * 1024 * 1024);
    let result = roomy.execute(QUERY).unwrap();
    assert_eq!(
        result
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        10_000
    );
    let expected = "x".repeat(1024);
    for batch in result.batches {
        for column in batch.columns() {
            let values = column.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(values.null_count(), 0);
            assert!(values.iter().all(|value| value == Some(expected.as_str())));
        }
    }
}
