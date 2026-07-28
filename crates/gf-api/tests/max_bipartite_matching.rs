//! Public Rust acceptance coverage for maximum bipartite matching.

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

fn options(partition_property: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::MaxBipartiteMatching,
        directed: false,
        partition_property: partition_property.map(str::to_owned),
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

fn node_names(graph: &GraphForge) -> BTreeMap<UuidBytes, String> {
    let result = graph
        .execute("MATCH (n:Person) RETURN n.node_uuid AS uuid, n.name AS name")
        .unwrap();
    let mut names = BTreeMap::new();
    for batch in result.batches {
        let uuids = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            names.insert(uuid_at(uuids, row), values.value(row).to_owned());
        }
    }
    names
}

fn selected_edges(graph: &GraphForge) -> BTreeSet<MatchingRow> {
    let result = graph
        .execute(
            "MATCH (a:Person)-[r:BIPARTITE]->(b:Person) \
             RETURN r.edge_uuid AS edge_uuid, a.node_uuid AS source_uuid, \
             b.node_uuid AS target_uuid",
        )
        .unwrap();
    let mut edges = BTreeSet::new();
    for batch in result.batches {
        let edge = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let source = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let target = batch
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            edges.insert((
                uuid_at(edge, row),
                uuid_at(source, row),
                uuid_at(target, row),
            ));
        }
    }
    edges
}

#[test]
fn explicit_partitions_return_a_stable_maximum_uuid_matching() {
    let (_dir, graph) = graph_with(
        "CREATE \
         (l1:Person {name:'l1', side:'a', bucket:1}), \
         (l2:Person {name:'l2', side:'a', bucket:1}), \
         (l3:Person {name:'l3', side:'a', bucket:1}), \
         (r1:Person {name:'r1', side:'z', bucket:2}), \
         (r2:Person {name:'r2', side:'z', bucket:2}), \
         (r3:Person {name:'r3', side:'z', bucket:2}), \
         (isolate:Person {name:'isolate', side:'a', bucket:1}), \
         (l1)-[:BIPARTITE]->(r1), \
         (l1)-[:BIPARTITE]->(r2), \
         (l1)-[:BIPARTITE]->(r2), \
         (l2)-[:BIPARTITE]->(r1), \
         (l3)-[:BIPARTITE]->(r2), \
         (l3)-[:BIPARTITE]->(r3)",
    );

    let string_partition = graph
        .analyze(Some("Person"), options(Some("side")))
        .unwrap();
    assert_eq!(
        string_partition.schema().fields(),
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
        string_partition.schema().metadata()["graphforge.algorithm"],
        "max_bipartite_matching"
    );
    assert_eq!(
        string_partition.schema().metadata()["graphforge.verb"],
        "analyze"
    );
    assert_eq!(
        string_partition.schema().metadata()["graphforge.algorithm_schema_version"],
        "1"
    );

    let rows = matching_rows(&string_partition);
    assert_eq!(rows.len(), 3, "the hand-verifiable graph has cardinality 3");
    assert!(rows.windows(2).all(|pair| pair[0] < pair[1]));
    let names = node_names(&graph);
    let topology = selected_edges(&graph);
    let mut sources = BTreeSet::new();
    let mut targets = BTreeSet::new();
    for &(edge, source, target) in &rows {
        assert!(topology.contains(&(edge, source, target)));
        assert_eq!(
            edge,
            topology
                .iter()
                .filter(|(_, candidate_source, candidate_target)| {
                    *candidate_source == source && *candidate_target == target
                })
                .map(|(candidate, _, _)| *candidate)
                .min()
                .unwrap(),
            "a matched endpoint pair must use its lowest canonical edge UUID"
        );
        assert!(names[&source].starts_with('l'));
        assert!(names[&target].starts_with('r'));
        assert!(sources.insert(source), "left endpoint matched twice");
        assert!(targets.insert(target), "right endpoint matched twice");
    }
    assert!(
        !rows.iter().any(|(_, source, target)| {
            names[source] == "isolate" || names[target] == "isolate"
        })
    );

    let repeated = graph
        .analyze(Some("Person"), options(Some("side")))
        .unwrap();
    assert_eq!(string_partition.schema(), repeated.schema());
    assert_eq!(string_partition.columns(), repeated.columns());

    let integer_partition = graph
        .analyze(Some("Person"), options(Some("bucket")))
        .unwrap();
    assert_eq!(string_partition.columns(), integer_partition.columns());
}

#[test]
fn inferred_partitions_are_canonical_across_disconnected_components() {
    let (_dir, graph) = graph_with(
        "CREATE \
         (a:Person {name:'a'}), (b:Person {name:'b'}), \
         (c:Person {name:'c'}), (d:Person {name:'d'}), \
         (isolate:Person {name:'isolate'}), \
         (b)-[:BIPARTITE]->(a), (d)-[:BIPARTITE]->(c)",
    );
    let first = graph.analyze(Some("Person"), options(None)).unwrap();
    let second = graph.analyze(Some("Person"), options(None)).unwrap();
    assert_eq!(first.columns(), second.columns());

    let names = node_names(&graph);
    let rows = matching_rows(&first);
    assert_eq!(rows.len(), 2);
    for (_, source, target) in rows {
        assert!(
            source < target,
            "the lowest UUID in each component is inferred as left"
        );
        assert_ne!(names[&source], "isolate");
        assert_ne!(names[&target], "isolate");
    }
}

#[test]
fn invalid_graphs_and_public_options_fail_atomically() {
    for (query, partition, expected) in [
        (
            "CREATE (a:Person {side:'x'}), (b:Person {side:'x'}), \
             (c:Person {side:'y'}), (a)-[:BIPARTITE]->(b), \
             (a)-[:BIPARTITE]->(c)",
            Some("side"),
            "selected edge connects nodes in the same partition",
        ),
        (
            "CREATE (a:Person), (b:Person), (c:Person), \
             (a)-[:BIPARTITE]->(b), (b)-[:BIPARTITE]->(c), \
             (c)-[:BIPARTITE]->(a)",
            None,
            "selected graph is not bipartite: odd cycle",
        ),
        (
            "CREATE (a:Person), (a)-[:BIPARTITE]->(a)",
            None,
            "selected graph is not bipartite: self-loop",
        ),
        (
            "CREATE (a:Person {side:'x'}), (b:Person), \
             (a)-[:BIPARTITE]->(b)",
            Some("side"),
            "missing a partition value",
        ),
        (
            "CREATE (a:Person {side:'x'}), (b:Person {side:null}), \
             (a)-[:BIPARTITE]->(b)",
            Some("side"),
            "missing a partition value",
        ),
        (
            "CREATE (a:Person {side:'x'}), (b:Person {side:1.5}), \
             (a)-[:BIPARTITE]->(b)",
            Some("side"),
            "unsupported partition type",
        ),
        (
            "CREATE (a:Person {side:'x'}), (b:Person {side:'x'}), \
             (a)-[:BIPARTITE]->(b)",
            Some("side"),
            "edge-bearing projection must contain exactly two partitions",
        ),
        (
            "CREATE (a:Person {side:'x'}), (b:Person {side:'y'}), \
             (c:Person {side:'z'}), (a)-[:BIPARTITE]->(b), \
             (b)-[:BIPARTITE]->(c)",
            Some("side"),
            "edge-bearing projection must contain exactly two partitions",
        ),
    ] {
        let (_dir, graph) = graph_with(query);
        let error = graph
            .analyze(Some("Person"), options(partition))
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "{error}");
    }

    let (_dir, graph) = graph_with(
        "CREATE (a:Person {side:'x'}), (b:Person {side:'y'}), \
         (a)-[:BIPARTITE]->(b)",
    );
    for (options, expected) in [
        (
            AnalyzeOptions {
                by: AnalyzeAlgorithm::MaxBipartiteMatching,
                partition_property: Some("side".into()),
                ..AnalyzeOptions::default()
            },
            "max_bipartite_matching requires directed=false",
        ),
        (
            AnalyzeOptions {
                weight: Some("weight".into()),
                ..options(Some("side"))
            },
            "max_bipartite_matching does not accept an edge weight property",
        ),
        (
            options(Some("")),
            "max_bipartite_matching requires a non-empty partition_property when supplied",
        ),
    ] {
        let error = graph.analyze(Some("Person"), options).unwrap_err();
        assert!(matches!(error, GfError::Validation(message) if message == expected));
    }
}
