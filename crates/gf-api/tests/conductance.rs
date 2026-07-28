//! Public Rust acceptance coverage for conductance.

use std::sync::Arc;

use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Fields};
use gf_api::GraphForge;
use gf_core::algorithms::AnalyzeAlgorithm;
use gf_core::{AnalyzeOptions, GfError};

fn graph_with(query: &str) -> (tempfile::TempDir, GraphForge) {
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    graph.execute(query).unwrap();
    (dir, graph)
}

fn options(partition_property: &str, weight: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::Conductance,
        directed: false,
        weight: weight.map(str::to_owned),
        partition_property: Some(partition_property.into()),
        ..AnalyzeOptions::default()
    }
}

fn values(batch: &arrow::record_batch::RecordBatch) -> Vec<(String, f64)> {
    let partitions = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let conductance = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| (partitions.value(row).to_owned(), conductance.value(row)))
        .collect()
}

#[test]
fn conductance_is_exact_deterministic_and_stably_shaped_through_public_api() {
    let (_dir, graph) = graph_with(
        "CREATE \
         (a:Person {side:'alpha', bucket:1}), \
         (b:Person {side:'alpha', bucket:1}), \
         (c:Person {side:'beta', bucket:2}), \
         (d:Person {side:'beta', bucket:2}), \
         (a)-[:LINK {weight:2}]->(c), \
         (a)-[:LINK {weight:1}]->(c), \
         (b)-[:LINK {weight:1}]->(c), \
         (a)-[:LINK {weight:3}]->(b), \
         (d)-[:LINK {weight:4}]->(d)",
    );

    let weighted = graph
        .analyze(Some("Person"), options("side", Some("weight")))
        .unwrap();
    assert_eq!(
        weighted.schema().fields(),
        &Fields::from(vec![
            Arc::new(Field::new("partition_id", DataType::Utf8, false)),
            Arc::new(Field::new("conductance", DataType::Float64, false)),
        ])
    );
    assert_eq!(
        weighted.schema().metadata()["graphforge.algorithm"],
        "conductance"
    );
    assert_eq!(weighted.schema().metadata()["graphforge.verb"], "analyze");
    assert_eq!(
        weighted.schema().metadata()["graphforge.algorithm_schema_version"],
        "1"
    );
    assert_eq!(
        values(&weighted),
        [("alpha".into(), 0.4), ("beta".into(), 0.4)]
    );
    let repeated = graph
        .analyze(Some("Person"), options("side", Some("weight")))
        .unwrap();
    assert_eq!(weighted.schema(), repeated.schema());
    assert_eq!(weighted.columns(), repeated.columns());

    let unit = graph
        .analyze(Some("Person"), options("side", None))
        .unwrap();
    assert_eq!(values(&unit), [("alpha".into(), 0.6), ("beta".into(), 0.6)]);
    let integer = graph
        .analyze(Some("Person"), options("bucket", Some("weight")))
        .unwrap();
    assert_eq!(values(&integer), [("1".into(), 0.4), ("2".into(), 0.4)]);
}

#[test]
fn conductance_rejects_invalid_public_options_and_partition_data() {
    let (_dir, graph) = graph_with(
        "CREATE (a:Person {side:'alpha'}), (b:Person {side:'beta'}), \
         (a)-[:LINK]->(b)",
    );
    let directed = graph
        .analyze(
            Some("Person"),
            AnalyzeOptions {
                by: AnalyzeAlgorithm::Conductance,
                partition_property: Some("side".into()),
                ..AnalyzeOptions::default()
            },
        )
        .unwrap_err();
    assert!(matches!(
        directed,
        GfError::Validation(message) if message == "conductance requires directed=false"
    ));
    let missing_option = graph
        .analyze(
            Some("Person"),
            AnalyzeOptions {
                by: AnalyzeAlgorithm::Conductance,
                directed: false,
                ..AnalyzeOptions::default()
            },
        )
        .unwrap_err();
    assert!(matches!(
        missing_option,
        GfError::Validation(message)
            if message == "conductance requires a non-empty partition_property"
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
        let query = format!("CREATE (a:Person {{side:'alpha'}}), {node}, (a)-[:LINK]->(b)");
        let (_dir, invalid) = graph_with(&query);
        let error = invalid
            .analyze(Some("Person"), options("side", None))
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "{error}");
    }
}

#[test]
fn conductance_rejects_undefined_or_invalid_weighted_projections() {
    let (_dir, one_partition) =
        graph_with("CREATE (a:Person {side:'alpha'}), (b:Person {side:'alpha'}), (a)-[:LINK]->(b)");
    assert!(
        one_partition
            .analyze(Some("Person"), options("side", None))
            .unwrap_err()
            .to_string()
            .contains("requires two non-empty partitions")
    );

    let (_dir, zero_volume) =
        graph_with("CREATE (a:Person {side:'alpha'}), (b:Person {side:'beta'})");
    assert!(
        zero_volume
            .analyze(Some("Person"), options("side", None))
            .unwrap_err()
            .to_string()
            .contains("conductance is undefined for partition alpha")
    );

    let (_dir, missing_weight) = graph_with(
        "CREATE (a:Person {side:'alpha'}), (b:Person {side:'beta'}), \
         (a)-[:LINK]->(b)",
    );
    assert!(matches!(
        missing_weight
            .analyze(Some("Person"), options("side", Some("weight")))
            .unwrap_err(),
        GfError::Validation(message)
            if message.starts_with("edge weight is missing, NULL, NaN, or infinite for edge ")
    ));

    let (_dir, negative_weight) = graph_with(
        "CREATE (a:Person {side:'alpha'}), (b:Person {side:'beta'}), \
         (a)-[:LINK {weight:-1}]->(b)",
    );
    assert!(matches!(
        negative_weight
            .analyze(Some("Person"), options("side", Some("weight")))
            .unwrap_err(),
        GfError::Execution(message)
            if message
                == "Rust algorithm execution failed: conductance weights must be finite and nonnegative"
    ));
}
