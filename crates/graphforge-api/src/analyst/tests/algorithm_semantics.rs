use super::*;
use std::collections::{BTreeMap, HashMap};

fn vertices(graph: &GraphForge, labels: &[i64]) -> Vec<NodeHandle> {
    labels
        .iter()
        .enumerate()
        .map(|(index, &label)| {
            graph
                .add_node(
                    "Vertex",
                    &HashMap::from([
                        ("original_id".into(), PropValue::Int(label)),
                        ("id".into(), PropValue::Int(i64::try_from(index).unwrap())),
                    ]),
                )
                .unwrap()
        })
        .collect()
}

fn scores(batch: &arrow::record_batch::RecordBatch) -> BTreeMap<[u8; 16], f64> {
    let ids = batch
        .column_by_name("node_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let values = batch
        .column_by_name("score")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| (ids.value(row).try_into().unwrap(), values.value(row)))
        .collect()
}

fn labels(batch: &arrow::record_batch::RecordBatch) -> BTreeMap<[u8; 16], i64> {
    let ids = batch
        .column_by_name("node_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let values = batch
        .column_by_name("community_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| (ids.value(row).try_into().unwrap(), values.value(row)))
        .collect()
}

fn synchronous(iterations: u32) -> ClusterOptions {
    ClusterOptions {
        by: ClusterAlgorithm::LabelPropagation,
        directed: true,
        via: Some("EDGE".into()),
        synchronous_label_propagation: Some(SynchronousLabelPropagationOptions {
            iterations,
            initial_label_property: Some("original_id".into()),
        }),
        ..Default::default()
    }
}

#[test]
fn fixed_pagerank_facade_handles_sinks_filters_writeback_and_descriptors() {
    let graph = GraphForge::new(None).unwrap();
    let nodes = vertices(&graph, &[10, 20, 30]);
    graph
        .add_edge(&nodes[0], "EDGE", &nodes[1], &HashMap::new())
        .unwrap();
    graph
        .add_edge(&nodes[0], "IGNORED", &nodes[2], &HashMap::new())
        .unwrap();
    graph.add_node("Outside", &HashMap::new()).unwrap();
    let mut options = RankOptions {
        by: RankAlgorithm::PageRank,
        via: Some("EDGE".into()),
        pagerank: Some(PageRankOptions {
            damping: 0.6,
            iterations: Some(1),
        }),
        ..Default::default()
    };
    let descriptor = graph.prepare_rank_invocation("Vertex", &options).unwrap();
    assert_eq!(
        descriptor.parameters()["iterations"],
        InvocationParameter::U64(1)
    );
    assert_eq!(
        descriptor.parameters()["damping"],
        InvocationParameter::F64(0.6)
    );
    let prepared = graph.invoke_rank_descriptor(&descriptor).unwrap();
    options.write_property = Some("fixed_score".into());
    let output = graph.rank("Vertex", options.clone()).unwrap();
    assert_eq!(scores(&prepared), scores(&output));
    for (node, expected) in nodes.iter().zip([4.0 / 15.0, 7.0 / 15.0, 4.0 / 15.0]) {
        assert!((scores(&output)[node.uuid.as_bytes()] - expected).abs() < 1e-12);
    }
    let stored = graph
        .execute("MATCH (v:Vertex) RETURN v.fixed_score AS score ORDER BY v.id")
        .unwrap();
    let values = stored.batches[0]
        .column_by_name("score")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(values.len(), 3);
    assert!((values.value(1) - 7.0 / 15.0).abs() < 1e-12);
    options.write_property = None;
    options.pagerank.as_mut().unwrap().iterations = Some(0);
    for value in scores(&graph.rank("Vertex", options).unwrap()).values() {
        assert!((*value - 1.0 / 3.0).abs() < 1e-12);
    }
}

#[test]
fn synchronous_cdlp_preserves_exact_labels_reciprocal_votes_and_provenance() {
    let graph = GraphForge::new(None).unwrap();
    let large = 9_007_199_254_740_993;
    let nodes = vertices(&graph, &[large, 20, 10, 99]);
    for (source, target) in [(0, 1), (1, 0), (0, 2)] {
        graph
            .add_edge(&nodes[source], "EDGE", &nodes[target], &HashMap::new())
            .unwrap();
    }
    graph
        .add_edge(&nodes[3], "IGNORED", &nodes[0], &HashMap::new())
        .unwrap();
    let outside = graph.add_node("Outside", &HashMap::new()).unwrap();
    graph
        .add_edge(&nodes[0], "EDGE", &outside, &HashMap::new())
        .unwrap();
    let options = synchronous(1);
    let descriptor = graph
        .prepare_cluster_invocation("Vertex", &options)
        .unwrap();
    assert_eq!(
        descriptor.parameters()["directed"],
        InvocationParameter::Bool(true)
    );
    assert_eq!(
        descriptor.parameters()["synchronous_iterations"],
        InvocationParameter::U64(1)
    );
    let output = graph.invoke_cluster_descriptor(&descriptor).unwrap();
    for (node, expected) in nodes.iter().zip([20, large, large, 99]) {
        assert_eq!(labels(&output)[node.uuid.as_bytes()], expected);
    }
    let mut write = options.clone();
    write.write_property = Some("synchronous_group".into());
    assert_eq!(
        labels(&output),
        labels(&graph.cluster("Vertex", write).unwrap())
    );
    let stored = graph
        .execute("MATCH (v:Vertex) RETURN v.synchronous_group AS group_id ORDER BY v.id")
        .unwrap();
    let values = stored.batches[0]
        .column_by_name("group_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values().as_ref(), &[20, large, large, 99]);
    graph
        .execute("MATCH (v:Vertex {id: 1}) SET v.original_id = 21")
        .unwrap();
    assert_eq!(
        graph
            .invoke_cluster_descriptor(&descriptor)
            .unwrap_err()
            .code(),
        "GF_PROJECTION_CHANGED"
    );
}

#[test]
fn synchronous_cdlp_ties_follow_original_labels_independent_of_insertion_order() {
    for order in [[50, 20, 10], [10, 50, 20]] {
        let graph = GraphForge::new(None).unwrap();
        let nodes = vertices(&graph, &order);
        let by_label: BTreeMap<_, _> = order.into_iter().zip(nodes.iter()).collect();
        graph
            .add_edge(by_label[&50], "EDGE", by_label[&20], &HashMap::new())
            .unwrap();
        graph
            .add_edge(by_label[&50], "EDGE", by_label[&10], &HashMap::new())
            .unwrap();
        let output = graph.cluster("Vertex", synchronous(1)).unwrap();
        assert_eq!(labels(&output)[by_label[&50].uuid.as_bytes()], 10);
    }
}

#[test]
fn alternate_lcc_is_distinct_and_descriptor_replays_same_formula() {
    let graph = GraphForge::new(None).unwrap();
    let nodes = vertices(&graph, &[10, 20, 30, 40]);
    for (source, target) in [(0, 1), (1, 0), (0, 2), (1, 2), (0, 3)] {
        graph
            .add_edge(&nodes[source], "EDGE", &nodes[target], &HashMap::new())
            .unwrap();
    }
    let options = RankOptions {
        by: RankAlgorithm::ClusteringCoefficient,
        clustering_normalization: Some(ClusteringNormalization::NeighborEdges),
        via: Some("EDGE".into()),
        ..Default::default()
    };
    let output = graph.rank("Vertex", options.clone()).unwrap();
    assert_eq!(scores(&output)[nodes[0].uuid.as_bytes()], 1.0 / 6.0);
    let descriptor = graph.prepare_rank_invocation("Vertex", &options).unwrap();
    assert_eq!(
        scores(&output),
        scores(&graph.invoke_rank_descriptor(&descriptor).unwrap())
    );
    let mut writeback = options.clone();
    writeback.write_property = Some("neighbor_lcc".into());
    assert_eq!(
        scores(&output),
        scores(&graph.rank("Vertex", writeback).unwrap())
    );
    let stored = graph
        .execute("MATCH (v:Vertex) RETURN v.neighbor_lcc AS coefficient ORDER BY v.id")
        .unwrap();
    let values = stored.batches[0]
        .column_by_name("coefficient")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((values.value(0) - 1.0 / 6.0).abs() < 1e-12);
    let defaults = RankOptions {
        clustering_normalization: None,
        ..options
    };
    assert_ne!(
        scores(&output),
        scores(&graph.rank("Vertex", defaults).unwrap())
    );
}

#[test]
fn semantic_options_fail_typed_before_writeback() {
    let graph = GraphForge::new(None).unwrap();
    vertices(&graph, &[10]);
    for damping in [f64::NAN, f64::INFINITY, -0.1, 1.1] {
        let options = RankOptions {
            by: RankAlgorithm::PageRank,
            pagerank: Some(PageRankOptions {
                damping,
                iterations: Some(1),
            }),
            write_property: Some("bad_result".into()),
            ..Default::default()
        };
        assert!(matches!(
            graph.rank("Vertex", options),
            Err(GfError::Validation(_))
        ));
    }
    assert!(matches!(
        graph.rank(
            "Vertex",
            RankOptions {
                by: RankAlgorithm::Degree,
                pagerank: Some(PageRankOptions::default()),
                ..Default::default()
            }
        ),
        Err(GfError::Validation(_))
    ));
    assert!(matches!(
        graph.cluster(
            "Vertex",
            ClusterOptions {
                by: ClusterAlgorithm::Components,
                ..synchronous(1)
            }
        ),
        Err(GfError::Validation(_))
    ));
    let mut missing = synchronous(1);
    missing
        .synchronous_label_propagation
        .as_mut()
        .unwrap()
        .initial_label_property = Some("missing".into());
    assert!(matches!(
        graph.cluster("Vertex", missing),
        Err(GfError::Validation(_))
    ));
    for value in [
        PropValue::Float(1.5),
        PropValue::Float(1.0),
        PropValue::Null,
        PropValue::Str("10".into()),
    ] {
        let invalid = GraphForge::new(None).unwrap();
        invalid
            .add_node("Vertex", &HashMap::from([("original_id".into(), value)]))
            .unwrap();
        let mut options = synchronous(1);
        options.write_property = Some("bad_result".into());
        let invalid_labels = invalid.cluster("Vertex", options);
        assert!(
            matches!(invalid_labels, Err(GfError::Validation(_))),
            "{invalid_labels:?}"
        );
        let stored = invalid
            .execute("MATCH (v:Vertex) RETURN v.bad_result AS result")
            .unwrap();
        assert!(
            stored.batches[0]
                .column_by_name("result")
                .unwrap()
                .is_null(0)
        );
    }
    let batch = graph
        .execute("MATCH (v:Vertex) RETURN v.bad_result AS result")
        .unwrap();
    assert!(
        batch.batches[0]
            .column_by_name("result")
            .unwrap()
            .is_null(0)
    );
}
