//! Public Rust acceptance coverage for modularity.

use arrow::array::Float64Array;
use arrow::datatypes::{DataType, Field, Fields};
use graphforge_api::GraphForge;
use graphforge_core::algorithms::AnalyzeAlgorithm;
use graphforge_core::{AnalyzeOptions, GfError};
use std::sync::Arc;

fn graph_with(query: &str) -> (tempfile::TempDir, GraphForge) {
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    graph.execute(query).unwrap();
    (dir, graph)
}

fn options(partition_property: &str, weight: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::Modularity,
        directed: false,
        weight: weight.map(str::to_owned),
        partition_property: Some(partition_property.into()),
        ..AnalyzeOptions::default()
    }
}

fn score(batch: &arrow::record_batch::RecordBatch) -> f64 {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0)
}

#[test]
fn modularity_is_exact_deterministic_and_stably_shaped_through_persisted_api() {
    let (dir, graph) = graph_with(
        "CREATE \
         (a:Person {side:'alpha', bucket:1}), \
         (b:Person {side:'alpha', bucket:1}), \
         (c:Person {side:'beta', bucket:2}), \
         (d:Person {side:'beta', bucket:2}), \
         (a)-[:LINK {weight:2}]->(b), \
         (a)-[:LINK {weight:1}]->(b), \
         (c)-[:LINK {weight:2}]->(d), \
         (b)-[:LINK {weight:1}]->(c), \
         (a)-[:LINK {weight:3}]->(a)",
    );
    let weighted = graph
        .analyze(Some("Person"), options("side", Some("weight")))
        .unwrap();
    assert_eq!(
        weighted.schema().fields(),
        &Fields::from(vec![Arc::new(Field::new(
            "modularity",
            DataType::Float64,
            false,
        ))])
    );
    assert_eq!(
        weighted.schema().metadata()["graphforge.algorithm"],
        "modularity"
    );
    assert_eq!(weighted.schema().metadata()["graphforge.verb"], "analyze");
    assert_eq!(
        weighted.schema().metadata()["graphforge.algorithm_schema_version"],
        "1"
    );
    let weighted_expected =
        6.0 / 9.0 - (13.0_f64 / 18.0).powi(2) + 2.0 / 9.0 - (5.0_f64 / 18.0).powi(2);
    assert_eq!(score(&weighted), weighted_expected);
    assert_eq!(
        weighted,
        graph
            .analyze(Some("Person"), options("bucket", Some("weight")))
            .unwrap()
    );

    drop(graph);
    let reopened = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    assert_eq!(
        weighted,
        reopened
            .analyze(Some("Person"), options("side", Some("weight")))
            .unwrap()
    );
    assert_eq!(
        score(
            &reopened
                .analyze(Some("Person"), options("side", None))
                .unwrap()
        ),
        3.0 / 5.0 - (7.0_f64 / 10.0).powi(2) + 1.0 / 5.0 - (3.0_f64 / 10.0).powi(2)
    );
}

#[test]
fn modularity_rejects_invalid_options_partitions_weights_and_zero_volume() {
    let (_dir, graph) =
        graph_with("CREATE (a:Person {side:'alpha'}), (b:Person {side:'beta'}), (a)-[:LINK]->(b)");
    assert!(matches!(
        graph.analyze(
            Some("Person"),
            AnalyzeOptions {
                by: AnalyzeAlgorithm::Modularity,
                partition_property: Some("side".into()),
                ..AnalyzeOptions::default()
            }
        ),
        Err(GfError::Validation(message)) if message == "modularity requires directed=false"
    ));
    assert!(matches!(
        graph.analyze(
            Some("Person"),
            AnalyzeOptions {
                by: AnalyzeAlgorithm::Modularity,
                directed: false,
                ..AnalyzeOptions::default()
            }
        ),
        Err(GfError::Validation(message))
            if message == "modularity requires a non-empty partition_property"
    ));

    for (properties, expected) in [
        ("", "missing a partition value"),
        ("side:null", "missing a partition value"),
        ("side:1.5", "unsupported partition type"),
    ] {
        let node = if properties.is_empty() {
            "(b:Person)".to_owned()
        } else {
            format!("(b:Person {{{properties}}})")
        };
        let (_dir, invalid) = graph_with(&format!(
            "CREATE (a:Person {{side:'alpha'}}), {node}, (a)-[:LINK]->(b)"
        ));
        let error = invalid
            .analyze(Some("Person"), options("side", None))
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "{error}");
    }

    let (_dir, zero) = graph_with("CREATE (a:Person {side:'alpha'}), (b:Person {side:'beta'})");
    assert!(
        zero.analyze(Some("Person"), options("side", None))
            .unwrap_err()
            .to_string()
            .contains("modularity is undefined: total edge weight is zero")
    );

    let (_dir, missing_weight) =
        graph_with("CREATE (a:Person {side:'alpha'}), (b:Person {side:'beta'}), (a)-[:LINK]->(b)");
    assert!(matches!(
        missing_weight.analyze(Some("Person"), options("side", Some("weight"))),
        Err(GfError::Validation(message))
            if message.starts_with("edge weight is missing, NULL, NaN, or infinite for edge ")
    ));

    let (_dir, negative) = graph_with(
        "CREATE (a:Person {side:'alpha'}), (b:Person {side:'beta'}), \
         (a)-[:LINK {weight:-1}]->(b)",
    );
    assert!(matches!(
        negative.analyze(Some("Person"), options("side", Some("weight"))),
        Err(GfError::Execution(message))
            if message
                == "Rust algorithm execution failed: modularity weights must be finite and nonnegative"
    ));
}
