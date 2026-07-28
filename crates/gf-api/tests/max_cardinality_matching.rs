//! Public Rust acceptance coverage for exact maximum-cardinality matching.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{Array, FixedSizeBinaryArray, StringArray};
use arrow::datatypes::{DataType, Field, Fields};
use gf_api::GraphForge;
use gf_core::algorithms::AnalyzeAlgorithm;
use gf_core::{AnalyzeOptions, GfError};

type UuidBytes = [u8; 16];
type MatchingRow = (UuidBytes, UuidBytes, UuidBytes);

fn graph_with(query: &str) -> (tempfile::TempDir, GraphForge) {
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    graph.execute(query).unwrap();
    (dir, graph)
}

fn options() -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::MaxCardinalityMatching,
        directed: false,
        ..AnalyzeOptions::default()
    }
}

fn uuid_at(array: &FixedSizeBinaryArray, row: usize) -> UuidBytes {
    array.value(row).try_into().unwrap()
}

fn matching_rows(batch: &arrow::record_batch::RecordBatch) -> Vec<MatchingRow> {
    let columns = (0..3)
        .map(|column| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
        })
        .collect::<Vec<_>>();
    (0..batch.num_rows())
        .map(|row| {
            (
                uuid_at(columns[0], row),
                uuid_at(columns[1], row),
                uuid_at(columns[2], row),
            )
        })
        .collect()
}

fn edges_by_tag(graph: &GraphForge) -> BTreeMap<String, MatchingRow> {
    let result = graph
        .execute(
            "MATCH (a:Node)-[r:MATCH]->(b:Node) \
             RETURN r.tag AS tag, r.edge_uuid AS edge_uuid, \
             a.node_uuid AS source_uuid, b.node_uuid AS target_uuid",
        )
        .unwrap();
    let mut edges = BTreeMap::new();
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
            edges.insert(
                tags.value(row).to_owned(),
                (uuid_at(edge_ids, row), source, target),
            );
        }
    }
    edges
}

fn exact_oracle(edges: impl IntoIterator<Item = MatchingRow>) -> Vec<MatchingRow> {
    let mut edges = edges
        .into_iter()
        .filter(|(_, source, target)| source != target)
        .collect::<Vec<_>>();
    edges.sort_by_key(|&(edge, _, _)| edge);
    let mut best = Vec::new();
    for mask in 0..(1_u64 << edges.len()) {
        let mut used = BTreeSet::new();
        let mut candidate = Vec::new();
        let mut valid = true;
        for (position, &(_, source, target)) in edges.iter().enumerate() {
            if mask & (1 << position) == 0 {
                continue;
            }
            if !used.insert(source) || !used.insert(target) {
                valid = false;
                break;
            }
            candidate.push(position);
        }
        if !valid {
            continue;
        }
        if candidate.len() > best.len() || (candidate.len() == best.len() && candidate < best) {
            best = candidate;
        }
    }
    best.into_iter().map(|position| edges[position]).collect()
}

#[test]
fn persisted_multigraph_returns_exact_stable_uuid_matching() {
    let (_dir, graph) = graph_with(
        "CREATE \
         (a:Node), (b:Node), (c:Node), (d:Node), \
         (e:Node), (f:Node), (g:Node), (h:Node), \
         (a)-[:MATCH {tag:'ab0'}]->(b), \
         (a)-[:MATCH {tag:'ab1'}]->(b), \
         (b)-[:MATCH {tag:'bc'}]->(c), \
         (c)-[:MATCH {tag:'ca'}]->(a), \
         (b)-[:MATCH {tag:'bd'}]->(d), \
         (c)-[:MATCH {tag:'ce'}]->(e), \
         (f)-[:MATCH {tag:'fg'}]->(g), \
         (h)-[:MATCH {tag:'loop'}]->(h)",
    );
    let edges = edges_by_tag(&graph);
    let first = graph.analyze(Some("Node"), options()).unwrap();

    assert_eq!(
        first.schema().fields(),
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
        ])
    );
    assert_eq!(
        first.schema().metadata()["graphforge.algorithm"],
        "max_cardinality_matching"
    );
    assert_eq!(first.schema().metadata()["graphforge.verb"], "analyze");
    assert_eq!(
        first.schema().metadata()["graphforge.algorithm_schema_version"],
        "1"
    );
    assert!(
        first
            .columns()
            .iter()
            .all(|column| column.null_count() == 0)
    );
    for forbidden in [
        "weight",
        "confidence",
        "provenance_id",
        "assertion_uuid",
        "belief_status",
        "valid_time",
    ] {
        assert!(first.column_by_name(forbidden).is_none(), "{forbidden}");
    }

    let rows = matching_rows(&first);
    let expected = exact_oracle(edges.values().copied());
    assert_eq!(rows, expected);
    assert_eq!(rows.len(), 3);
    assert!(
        rows.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "rows must be ordered by ascending raw edge UUID"
    );
    let selected = rows.iter().map(|row| row.0).collect::<BTreeSet<_>>();
    assert!(!selected.contains(&edges["loop"].0));
    assert_eq!(
        selected
            .intersection(&BTreeSet::from([edges["ab0"].0, edges["ab1"].0]))
            .count(),
        1
    );
    let mut used = BTreeSet::new();
    for &(_, source, target) in &rows {
        assert!(source < target);
        assert!(used.insert(source));
        assert!(used.insert(target));
    }

    let repeated = graph.analyze(Some("Node"), options()).unwrap();
    assert_eq!(first.schema(), repeated.schema());
    assert_eq!(first.columns(), repeated.columns());
}

#[test]
fn empty_edgeless_and_invalid_options_are_stable() {
    for query in ["CREATE (:Node)", "CREATE (:Node), (:Node)"] {
        let (_dir, graph) = graph_with(query);
        let result = graph.analyze(Some("Node"), options()).unwrap();
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.num_columns(), 3);
    }

    let (_dir, graph) = graph_with("CREATE (a:Node), (b:Node), (a)-[:MATCH]->(b)");
    for (invalid, expected) in [
        (
            AnalyzeOptions {
                by: AnalyzeAlgorithm::MaxCardinalityMatching,
                ..AnalyzeOptions::default()
            },
            "max_cardinality_matching requires directed=false",
        ),
        (
            AnalyzeOptions {
                weight: Some("weight".into()),
                ..options()
            },
            "max_cardinality_matching does not accept an edge weight property",
        ),
    ] {
        let error = graph.analyze(Some("Node"), invalid).unwrap_err();
        assert!(matches!(error, GfError::Validation(message) if message == expected));
    }
}
