//! Public Rust acceptance coverage for minimum-k spanning trees.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{Array, FixedSizeBinaryArray, Float64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Fields};
use graphforge_api::{GraphForge, IrLiteral};
use graphforge_core::algorithms::AnalyzeAlgorithm;
use graphforge_core::{AnalyzeOptions, GfError};

fn graph_with(query: &str) -> (tempfile::TempDir, GraphForge) {
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    if !query.is_empty() {
        graph.execute(query).unwrap();
    }
    drop(graph);
    let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    (dir, graph)
}

fn options(k: Option<usize>, weight: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::MinimumKSpanningTree,
        directed: false,
        k,
        weight: weight.map(str::to_owned),
        ..AnalyzeOptions::default()
    }
}

fn edge_ids_by_tag(graph: &GraphForge) -> HashMap<String, Vec<u8>> {
    let result = graph
        .execute("MATCH ()-[r:LINK]->() RETURN r.tag AS tag, r.edge_uuid AS edge_uuid")
        .unwrap();
    let batch = &result.batches[0];
    let tags = batch
        .column_by_name("tag")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let ids = batch
        .column_by_name("edge_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| (tags.value(row).to_owned(), ids.value(row).to_vec()))
        .collect()
}

fn tree_edges(batch: &arrow::record_batch::RecordBatch) -> Vec<Vec<Vec<u8>>> {
    let tree_ids = batch
        .column_by_name("tree_id")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let edge_ids = batch
        .column_by_name("edge_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let mut trees = BTreeMap::<u64, Vec<Vec<u8>>>::new();
    for row in 0..batch.num_rows() {
        trees
            .entry(tree_ids.value(row))
            .or_default()
            .push(edge_ids.value(row).to_vec());
    }
    trees.into_values().collect()
}

fn sorted_tree(mut edges: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    edges.sort();
    edges
}

#[test]
fn public_api_returns_distinct_cheapest_trees_with_stable_shape() {
    let (_dir, graph) = graph_with(
        "CREATE \
         (a:TreeNode), (b:TreeNode), (c:TreeNode), \
         (a)-[:LINK {tag:'ab0', weight:1}]->(b), \
         (a)-[:LINK {tag:'ab1', weight:1}]->(b), \
         (a)-[:LINK {tag:'ac', weight:1}]->(c), \
         (b)-[:LINK {tag:'bc', weight:2}]->(c), \
         (a)-[:LINK {tag:'loop', weight:0}]->(a)",
    );
    let ids = edge_ids_by_tag(&graph);

    let default = graph
        .analyze(Some("TreeNode"), options(None, Some("weight")))
        .unwrap();
    assert_eq!(default.num_rows(), 2);
    assert_eq!(
        default.schema().fields(),
        &Fields::from(vec![
            Arc::new(Field::new("tree_id", DataType::UInt64, false)),
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
            Arc::new(Field::new("weight", DataType::Float64, false)),
        ])
    );
    assert_eq!(
        default.schema().metadata()["graphforge.algorithm"],
        "minimum_k_spanning_tree"
    );
    assert_eq!(default.schema().metadata()["graphforge.verb"], "analyze");
    assert_eq!(
        default.schema().metadata()["graphforge.algorithm_schema_version"],
        "1"
    );
    assert!(
        default
            .columns()
            .iter()
            .all(|column| column.null_count() == 0)
    );

    let mut expected = vec![
        sorted_tree(vec![ids["ab0"].clone(), ids["ac"].clone()]),
        sorted_tree(vec![ids["ab1"].clone(), ids["ac"].clone()]),
    ];
    expected.sort();
    assert_eq!(tree_edges(&default), vec![expected[0].clone()]);

    let all = graph
        .analyze(Some("TreeNode"), options(Some(99), Some("weight")))
        .unwrap();
    let actual = tree_edges(&all);
    assert_eq!(actual.len(), 5);
    assert_eq!(&actual[..2], expected.as_slice());
    assert!(!actual.iter().flatten().any(|edge| edge == &ids["loop"]));
    let weights = all
        .column_by_name("weight")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let tree_ids = all
        .column_by_name("tree_id")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let totals = (0..all.num_rows()).fold(BTreeMap::<u64, f64>::new(), |mut sums, row| {
        *sums.entry(tree_ids.value(row)).or_default() += weights.value(row);
        sums
    });
    assert_eq!(
        totals.into_values().collect::<Vec<_>>(),
        [2.0, 2.0, 3.0, 3.0, 3.0]
    );

    let repeated = graph
        .analyze(Some("TreeNode"), options(Some(99), Some("weight")))
        .unwrap();
    assert_eq!(all.schema(), repeated.schema());
    assert_eq!(all.columns(), repeated.columns());
}

#[test]
fn public_api_preserves_empty_singleton_and_connectivity_boundaries() {
    let (_dir, empty) = graph_with("");
    let batch = empty.analyze(None, options(None, None)).unwrap();
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.num_columns(), 5);

    let (_dir, singleton) = graph_with("CREATE (:Solo)");
    let batch = singleton
        .analyze(Some("Solo"), options(Some(3), None))
        .unwrap();
    assert_eq!(batch.num_rows(), 0);

    let (_dir, disconnected) = graph_with("CREATE (:Node), (:Node)");
    let error = disconnected
        .analyze(Some("Node"), options(None, None))
        .unwrap_err()
        .to_string();
    assert!(error.contains("requires a connected graph"), "{error}");
}

#[test]
fn public_api_rejects_invalid_options_and_weights_structurally() {
    let (_dir, graph) = graph_with("CREATE (a:Node), (b:Node), (a)-[:LINK {weight:1}]->(b)");
    let directed = graph
        .analyze(
            Some("Node"),
            AnalyzeOptions {
                by: AnalyzeAlgorithm::MinimumKSpanningTree,
                ..AnalyzeOptions::default()
            },
        )
        .unwrap_err();
    assert!(matches!(
        directed,
        GfError::Validation(message)
            if message == "minimum_k_spanning_tree requires directed=false"
    ));
    let zero = graph
        .analyze(Some("Node"), options(Some(0), Some("weight")))
        .unwrap_err();
    assert!(matches!(
        zero,
        GfError::Validation(message)
            if message == "minimum_k_spanning_tree requires k greater than zero"
    ));

    for (property, expected) in [
        ("", "weight is missing, NULL, NaN, or infinite"),
        ("weight:null", "weight is missing, NULL, NaN, or infinite"),
        ("weight:'heavy'", "must be numeric"),
        ("weight:-1", "nonnegative"),
    ] {
        let props = (!property.is_empty())
            .then(|| format!(" {{{property}}}"))
            .unwrap_or_default();
        let query = format!("CREATE (a:Node), (b:Node), (a)-[:LINK{props}]->(b)");
        let (_dir, invalid) = graph_with(&query);
        let error = invalid
            .analyze(Some("Node"), options(None, Some("weight")))
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "{property:?}: {error}");
    }

    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let dir = tempfile::tempdir().unwrap();
        let invalid = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        invalid
            .execute_with_params(
                "CREATE (a:Node), (b:Node), (a)-[:LINK {weight:$weight}]->(b)",
                &HashMap::from([("weight".into(), IrLiteral::Float(value))]),
            )
            .unwrap();
        let error = invalid
            .analyze(Some("Node"), options(None, Some("weight")))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("weight is missing, NULL, NaN, or infinite"),
            "{value}: {error}"
        );
    }
}
