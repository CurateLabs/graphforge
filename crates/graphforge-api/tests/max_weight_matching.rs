//! Public Rust acceptance coverage for exact maximum-weight matching.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, FixedSizeBinaryArray, Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Fields};
use graphforge_api::{GraphForge, IrLiteral};
use graphforge_core::algorithms::AnalyzeAlgorithm;
use graphforge_core::{AnalyzeOptions, GfError};

type UuidBytes = [u8; 16];
type MatchingRow = (UuidBytes, UuidBytes, UuidBytes, f64);
type TopologyRow = (UuidBytes, UuidBytes, UuidBytes);

fn graph_with(query: &str) -> (tempfile::TempDir, GraphForge) {
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    graph.execute(query).unwrap();
    (dir, graph)
}

fn options(weight: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::MaxWeightMatching,
        directed: false,
        weight: weight.map(str::to_owned),
        ..AnalyzeOptions::default()
    }
}

fn uuid_at(array: &FixedSizeBinaryArray, row: usize) -> UuidBytes {
    array.value(row).try_into().unwrap()
}

fn matching_rows(batch: &arrow::record_batch::RecordBatch) -> Vec<MatchingRow> {
    let edge = batch
        .column_by_name("edge_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let source = batch
        .column_by_name("source_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let target = batch
        .column_by_name("target_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let weights = batch
        .column_by_name("weight")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| {
            (
                uuid_at(edge, row),
                uuid_at(source, row),
                uuid_at(target, row),
                weights.value(row),
            )
        })
        .collect()
}

fn edges_by_tag(graph: &GraphForge) -> HashMap<String, TopologyRow> {
    let result = graph
        .execute(
            "MATCH (a)-[r:MATCH]->(b) \
             RETURN r.tag AS tag, r.edge_uuid AS edge_uuid, \
             a.node_uuid AS source_uuid, b.node_uuid AS target_uuid",
        )
        .unwrap();
    let mut ids = HashMap::new();
    for batch in result.batches {
        let tags = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let edge_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let sources = batch
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let targets = batch
            .column(3)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let mut source = uuid_at(sources, row);
            let mut target = uuid_at(targets, row);
            if target < source {
                std::mem::swap(&mut source, &mut target);
            }
            ids.insert(
                tags.value(row).to_owned(),
                (uuid_at(edge_ids, row), source, target),
            );
        }
    }
    ids
}

fn matching_fixture() -> (tempfile::TempDir, GraphForge) {
    graph_with(
        "CREATE \
         (a:Node), (b:Node), (c:Node), (d:Node), \
         (e:Node), (f:Node), (g:Node), \
         (a)-[:MATCH {tag:'ab0', weight:10}]->(b), \
         (a)-[:MATCH {tag:'ab1', weight:10}]->(b), \
         (b)-[:MATCH {tag:'bc', weight:7}]->(c), \
         (c)-[:MATCH {tag:'ca', weight:6}]->(a), \
         (d)-[:MATCH {tag:'de', weight:5}]->(e), \
         (f)-[:MATCH {tag:'fg', weight:-2}]->(g), \
         (a)-[:MATCH {tag:'loop', weight:100}]->(a)",
    )
}

#[test]
fn exact_weighted_matching_is_uuid_only_stable_and_deterministic() {
    let (_dir, graph) = matching_fixture();
    let edges = edges_by_tag(&graph);
    let batch = graph
        .analyze(Some("Node"), options(Some("weight")))
        .unwrap();

    assert_eq!(
        batch.schema().fields(),
        &Fields::from(vec![
            Arc::new(Field::new(
                "edge_uuid",
                DataType::FixedSizeBinary(16),
                false,
            )),
            Arc::new(Field::new(
                "source_uuid",
                DataType::FixedSizeBinary(16),
                false,
            )),
            Arc::new(Field::new(
                "target_uuid",
                DataType::FixedSizeBinary(16),
                false,
            )),
            Arc::new(Field::new("weight", DataType::Float64, true)),
        ])
    );
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "max_weight_matching"
    );
    assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm_schema_version"],
        "1"
    );
    assert!(
        batch
            .columns()
            .iter()
            .all(|column| column.null_count() == 0)
    );

    let rows = matching_rows(&batch);
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|(_, source, target, _)| source < target));
    assert!(
        rows.windows(2)
            .all(|pair| { (pair[0].1, pair[0].2, pair[0].0) < (pair[1].1, pair[1].2, pair[1].0) })
    );
    let parallel = if edges["ab0"].0 < edges["ab1"].0 {
        edges["ab0"]
    } else {
        edges["ab1"]
    };
    let mut expected = vec![
        (parallel.0, parallel.1, parallel.2, 10.0),
        (edges["de"].0, edges["de"].1, edges["de"].2, 5.0),
    ];
    expected.sort_by_key(|row| (row.1, row.2, row.0));
    assert_eq!(rows, expected);
    assert!(
        !rows
            .iter()
            .any(|(edge, _, _, _)| [edges["loop"].0, edges["fg"].0].contains(edge))
    );

    let repeated = graph
        .analyze(Some("Node"), options(Some("weight")))
        .unwrap();
    assert_eq!(batch.schema(), repeated.schema());
    assert_eq!(batch.columns(), repeated.columns());
}

#[test]
fn omitted_weight_uses_unit_weights_distinct_from_max_cardinality() {
    let (_dir, graph) = matching_fixture();
    let weighted = graph
        .analyze(Some("Node"), options(Some("weight")))
        .unwrap();
    let unit = graph.analyze(Some("Node"), options(None)).unwrap();

    assert_eq!(weighted.num_rows(), 2);
    assert_eq!(unit.num_rows(), 3);
    assert!(
        matching_rows(&unit)
            .iter()
            .all(|(_, _, _, weight)| *weight == 1.0)
    );
    let repeated = graph.analyze(Some("Node"), options(None)).unwrap();
    assert_eq!(unit.columns(), repeated.columns());
}

#[test]
fn directed_and_nonfinite_inputs_are_structured_failures() {
    let (_dir, graph) = graph_with("CREATE (a:Node), (b:Node), (a)-[:MATCH {weight:1}]->(b)");
    let directed = graph
        .analyze(
            Some("Node"),
            AnalyzeOptions {
                by: AnalyzeAlgorithm::MaxWeightMatching,
                weight: Some("weight".into()),
                ..AnalyzeOptions::default()
            },
        )
        .unwrap_err();
    assert!(matches!(
        directed,
        GfError::Validation(message) if message == "max_weight_matching requires directed=false"
    ));

    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let dir = tempfile::tempdir().unwrap();
        let invalid = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        invalid
            .execute_with_params(
                "CREATE (a:Node), (b:Node), \
                 (a)-[:MATCH {weight:$weight}]->(b)",
                &HashMap::from([("weight".into(), IrLiteral::Float(value))]),
            )
            .unwrap();
        let error = invalid
            .analyze(Some("Node"), options(Some("weight")))
            .unwrap_err();
        assert!(
            matches!(
                error,
                GfError::Validation(ref message)
                    if message.starts_with("edge weight is missing, NULL, NaN, or infinite for edge ")
            ),
            "{value}: {error}"
        );
    }
}
