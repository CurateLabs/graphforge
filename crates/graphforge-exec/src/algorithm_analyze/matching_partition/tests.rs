use super::super::tests::execute;
use super::super::*;
use super::*;
use crate::algorithm_partition::PartitionValue;

fn conductance_partitions(graph: &AdjacencyGraph, values: &[(u8, &str)]) -> ResolvedPartitionMap {
    ResolvedPartitionMap::try_new(
        graph.node_uuids(),
        values.iter().map(|&(node, partition)| {
            (
                u128::from(node).to_be_bytes(),
                PartitionValue::String(partition.into()),
            )
        }),
    )
    .unwrap()
}

#[test]
fn conductance_dispatches_weighted_rows_with_stable_schema() {
    let graph = AdjacencyGraph::with_test_undirected_multigraph(
        4,
        &[(10, 0, 2), (11, 1, 2), (12, 0, 1), (13, 3, 3)],
    )
    .with_test_edge_weights(&[2.0, 2.0, 1.0, 1.0, 3.0, 3.0, 4.0]);
    let handler = Conductance {
        partitions: conductance_partitions(
            &graph,
            &[(0, "alpha"), (1, "alpha"), (2, "beta"), (3, "beta")],
        ),
    };
    let output = handler
        .execute(
            &graph,
            &AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default()),
        )
        .unwrap();
    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::Conductance).result_schema()
    );
    assert_eq!(
        output.rows(),
        [
            vec![
                AlgorithmValue::Utf8("alpha".into()),
                AlgorithmValue::Float64(1.0 / 3.0),
            ],
            vec![
                AlgorithmValue::Utf8("beta".into()),
                AlgorithmValue::Float64(1.0 / 3.0),
            ],
        ]
    );
    let batch =
        shape_algorithm_output(Algorithm::Analyze(AnalyzeAlgorithm::Conductance), &output).unwrap();
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.is_nullable()))
            .collect::<Vec<_>>(),
        [("partition_id", false), ("conductance", false)]
    );
}

#[test]
fn modularity_dispatches_weighted_scalar_with_stable_schema() {
    let graph = AdjacencyGraph::with_test_undirected_multigraph(
        4,
        &[(10, 0, 1), (11, 2, 3), (12, 1, 2), (13, 0, 0)],
    )
    .with_test_edge_weights(&[2.0, 2.0, 2.0, 2.0, 1.0, 1.0, 3.0]);
    let handler = Modularity {
        partitions: conductance_partitions(
            &graph,
            &[(0, "alpha"), (1, "alpha"), (2, "beta"), (3, "beta")],
        ),
    };
    let control =
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
    let output = handler.execute(&graph, &control).unwrap();

    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::Modularity).result_schema()
    );
    assert_eq!(output.rows().len(), 1);
    assert!(
        matches!(output.rows()[0].as_slice(), [AlgorithmValue::Float64(value)] if value.is_finite())
    );
    assert_eq!(
        shape_algorithm_output(Algorithm::Analyze(AnalyzeAlgorithm::Modularity), &output)
            .unwrap()
            .schema()
            .field(0)
            .is_nullable(),
        false
    );

    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        handler.execute(
            &graph,
            &AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
        ),
        Err(AlgorithmError::Cancelled)
    );
    assert!(matches!(
        handler.execute(
            &graph,
            &AlgorithmControl::new(
                AlgorithmLimits {
                    output_rows: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            )
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
}

#[test]
fn conductance_handler_propagates_zero_volume_cancellation_and_limits() {
    let graph = AdjacencyGraph::with_test_undirected_multigraph(2, &[]);
    let partitions = conductance_partitions(&graph, &[(0, "alpha"), (1, "beta")]);
    let handler = Conductance {
        partitions: partitions.clone(),
    };
    assert_eq!(
        handler.execute(
            &graph,
            &AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default(),),
        ),
        Err(AlgorithmError::UndefinedConductance {
            partition: "alpha".into(),
        })
    );

    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        Conductance {
            partitions: partitions.clone(),
        }
        .execute(
            &graph,
            &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
        ),
        Err(AlgorithmError::Cancelled)
    );

    let graph = AdjacencyGraph::with_test_undirected_multigraph(2, &[(1, 0, 1)]);
    assert!(matches!(
        Conductance {
            partitions: conductance_partitions(&graph, &[(0, "alpha"), (1, "beta")]),
        }
        .execute(
            &graph,
            &AlgorithmControl::new(
                AlgorithmLimits {
                    output_rows: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
}

#[test]
fn conductance_rejects_directed_dispatch_before_storage_reads() {
    let dir = tempfile::tempdir().unwrap();
    let provider =
        crate::ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
    assert!(matches!(
        analyze_algorithm(
            &provider,
            dir.path(),
            OntologyMode::Strict,
            EntityTypeSelection::All,
            &AnalyzeOptions {
                by: AnalyzeAlgorithm::Conductance,
                directed: true,
                partition_property: Some("side".into()),
                ..AnalyzeOptions::default()
            }
        ),
        Err(GfError::Validation(message))
            if message == "conductance requires directed=false"
    ));
}

#[test]
fn modularity_rejects_directed_dispatch_before_storage_reads() {
    let dir = tempfile::tempdir().unwrap();
    let provider =
        crate::ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
    assert!(matches!(
        analyze_algorithm(
            &provider,
            dir.path(),
            OntologyMode::Strict,
            EntityTypeSelection::All,
            &AnalyzeOptions {
                by: AnalyzeAlgorithm::Modularity,
                directed: true,
                partition_property: Some("community".into()),
                ..AnalyzeOptions::default()
            }
        ),
        Err(GfError::Validation(message))
            if message == "modularity requires directed=false"
    ));
}

#[test]
fn max_weight_matching_dispatches_canonical_weighted_uuid_rows() {
    let graph = AdjacencyGraph::with_test_undirected_multigraph(
        6,
        &[(9, 0, 1), (8, 0, 1), (7, 2, 3), (6, 4, 5), (10, 4, 4)],
    )
    .with_test_edge_weights(&[-1.0, -1.0, 4.0, 4.0, 5.0, 5.0, 5.0, 5.0, 100.0]);
    let output = execute(
        &graph,
        AnalyzeAlgorithm::MaxWeightMatching,
        false,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();

    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::MaxWeightMatching).result_schema()
    );
    assert_eq!(
        output.rows(),
        [
            vec![
                AlgorithmValue::Uuid(u128::from(8_u8).to_be_bytes()),
                AlgorithmValue::Uuid(u128::from(0_u8).to_be_bytes()),
                AlgorithmValue::Uuid(u128::from(1_u8).to_be_bytes()),
                AlgorithmValue::Float64(5.0),
            ],
            vec![
                AlgorithmValue::Uuid(u128::from(7_u8).to_be_bytes()),
                AlgorithmValue::Uuid(u128::from(2_u8).to_be_bytes()),
                AlgorithmValue::Uuid(u128::from(3_u8).to_be_bytes()),
                AlgorithmValue::Float64(4.0),
            ],
        ]
    );
    let batch = shape_algorithm_output(
        Algorithm::Analyze(AnalyzeAlgorithm::MaxWeightMatching),
        &output,
    )
    .unwrap();
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.is_nullable()))
            .collect::<Vec<_>>(),
        [
            ("edge_uuid", false),
            ("source_uuid", false),
            ("target_uuid", false),
            ("weight", true),
        ]
    );
}

#[test]
fn max_cardinality_matching_dispatches_stable_unweighted_uuid_rows() {
    let graph = AdjacencyGraph::with_test_undirected_multigraph(
        6,
        &[
            (9, 0, 1),
            (8, 0, 1),
            (7, 1, 2),
            (6, 2, 0),
            (5, 1, 3),
            (4, 2, 4),
            (3, 5, 5),
        ],
    );
    let output = execute(
        &graph,
        AnalyzeAlgorithm::MaxCardinalityMatching,
        false,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();

    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::MaxCardinalityMatching).result_schema()
    );
    assert_eq!(
        output.rows(),
        [
            vec![
                AlgorithmValue::Uuid(u128::from(4_u8).to_be_bytes()),
                AlgorithmValue::Uuid(u128::from(2_u8).to_be_bytes()),
                AlgorithmValue::Uuid(u128::from(4_u8).to_be_bytes()),
            ],
            vec![
                AlgorithmValue::Uuid(u128::from(5_u8).to_be_bytes()),
                AlgorithmValue::Uuid(u128::from(1_u8).to_be_bytes()),
                AlgorithmValue::Uuid(u128::from(3_u8).to_be_bytes()),
            ],
        ]
    );
    let batch = shape_algorithm_output(
        Algorithm::Analyze(AnalyzeAlgorithm::MaxCardinalityMatching),
        &output,
    )
    .unwrap();
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.is_nullable()))
            .collect::<Vec<_>>(),
        [
            ("edge_uuid", false),
            ("source_uuid", false),
            ("target_uuid", false)
        ]
    );
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "max_cardinality_matching"
    );
    for forbidden in [
        "weight",
        "confidence",
        "provenance_id",
        "assertion_uuid",
        "belief_status",
        "valid_time",
    ] {
        assert!(batch.column_by_name(forbidden).is_none(), "{forbidden}");
    }
}

#[test]
fn max_cardinality_matching_handles_empty_and_shared_controls() {
    let empty = execute(
        &AdjacencyGraph::default(),
        AnalyzeAlgorithm::MaxCardinalityMatching,
        false,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert!(empty.rows().is_empty());

    let graph = AdjacencyGraph::with_test_undirected_multigraph(4, &[(1, 0, 1), (2, 2, 3)]);
    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::MaxCardinalityMatching,
            false,
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::MaxCardinalityMatching,
            false,
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::IterationLimit { .. })
    ));
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute(
            &graph,
            AnalyzeAlgorithm::MaxCardinalityMatching,
            false,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
fn max_cardinality_matching_rejects_directed_before_storage_reads() {
    let dir = tempfile::tempdir().unwrap();
    let provider =
        crate::ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
    assert!(matches!(
        analyze_algorithm(
            &provider,
            dir.path(),
            OntologyMode::Strict,
            EntityTypeSelection::All,
            &AnalyzeOptions {
                by: AnalyzeAlgorithm::MaxCardinalityMatching,
                directed: true,
                ..AnalyzeOptions::default()
            }
        ),
        Err(GfError::Validation(message))
            if message == "max_cardinality_matching requires directed=false"
    ));
}

#[test]
fn max_weight_matching_handles_empty_and_shared_controls() {
    let empty = execute(
        &AdjacencyGraph::default(),
        AnalyzeAlgorithm::MaxWeightMatching,
        false,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert!(empty.rows().is_empty());

    let graph = AdjacencyGraph::with_test_undirected_multigraph(4, &[(1, 0, 1), (2, 2, 3)]);
    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::MaxWeightMatching,
            false,
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::MaxWeightMatching,
            false,
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::IterationLimit { .. })
    ));
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute(
            &graph,
            AnalyzeAlgorithm::MaxWeightMatching,
            false,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
fn max_weight_matching_rejects_directed_and_nonfinite_inputs() {
    let dir = tempfile::tempdir().unwrap();
    let provider =
        crate::ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
    assert!(matches!(
        analyze_algorithm(
            &provider,
            dir.path(),
            OntologyMode::Strict,
            EntityTypeSelection::All,
            &AnalyzeOptions {
                by: AnalyzeAlgorithm::MaxWeightMatching,
                directed: true,
                ..AnalyzeOptions::default()
            }
        ),
        Err(GfError::Validation(message))
            if message == "max_weight_matching requires directed=false"
    ));

    let graph = AdjacencyGraph::with_test_undirected_multigraph(2, &[(1, 0, 1)])
        .with_test_edge_weights(&[f64::NAN, f64::NAN]);
    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::MaxWeightMatching,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::Execution { message })
            if message == "weighted graph requires finite edge weights"
    ));
}

#[test]
fn bipartite_matching_dispatches_stable_unweighted_uuid_rows() {
    let graph = AdjacencyGraph::with_test_undirected_multigraph(
        5,
        &[(9, 0, 3), (3, 0, 3), (4, 0, 4), (5, 1, 3), (6, 2, 4)],
    );
    let output = execute(
        &graph,
        AnalyzeAlgorithm::MaxBipartiteMatching,
        false,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::MaxBipartiteMatching).result_schema()
    );
    assert_eq!(
        output.rows(),
        [
            vec![
                AlgorithmValue::Uuid(u128::from(3_u8).to_be_bytes()),
                AlgorithmValue::Uuid(u128::from(0_u8).to_be_bytes()),
                AlgorithmValue::Uuid(u128::from(3_u8).to_be_bytes()),
            ],
            vec![
                AlgorithmValue::Uuid(u128::from(6_u8).to_be_bytes()),
                AlgorithmValue::Uuid(u128::from(2_u8).to_be_bytes()),
                AlgorithmValue::Uuid(u128::from(4_u8).to_be_bytes()),
            ],
        ]
    );
    let batch = shape_algorithm_output(
        Algorithm::Analyze(AnalyzeAlgorithm::MaxBipartiteMatching),
        &output,
    )
    .unwrap();
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.is_nullable()))
            .collect::<Vec<_>>(),
        [
            ("edge_uuid", false),
            ("source_uuid", false),
            ("target_uuid", false)
        ]
    );
}

#[test]
fn bipartite_matching_uses_explicit_partition_orientation() {
    let graph = AdjacencyGraph::with_test_undirected_multigraph(2, &[(7, 0, 1)]);
    let partitions = ResolvedPartitionMap::try_new(
        graph.node_uuids(),
        [
            (
                u128::from(0_u8).to_be_bytes(),
                PartitionValue::String("z".into()),
            ),
            (
                u128::from(1_u8).to_be_bytes(),
                PartitionValue::String("a".into()),
            ),
        ],
    )
    .unwrap();
    let output = MaxBipartiteMatching {
        partitions: Some(partitions),
    }
    .execute(
        &graph,
        &AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default()),
    )
    .unwrap();
    assert_eq!(
        output.rows()[0][1..],
        [
            AlgorithmValue::Uuid(u128::from(1_u8).to_be_bytes()),
            AlgorithmValue::Uuid(u128::from(0_u8).to_be_bytes())
        ]
    );
}

#[test]
fn bipartite_matching_rejects_directed_dispatch_before_storage_reads() {
    let dir = tempfile::tempdir().unwrap();
    let provider =
        crate::ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
    assert!(matches!(
        analyze_algorithm(
            &provider,
            dir.path(),
            OntologyMode::Strict,
            EntityTypeSelection::All,
            &AnalyzeOptions {
                by: AnalyzeAlgorithm::MaxBipartiteMatching,
                directed: true,
                ..AnalyzeOptions::default()
            }
        ),
        Err(GfError::Validation(message))
            if message == "max_bipartite_matching requires directed=false"
    ));
}

#[test]
fn bipartite_matching_handler_observes_pre_cancellation() {
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        MaxBipartiteMatching { partitions: None }.execute(
            &AdjacencyGraph::with_test_undirected_multigraph(2, &[(1, 0, 1)]),
            &AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
        ),
        Err(AlgorithmError::Cancelled)
    );
}
