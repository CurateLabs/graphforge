use super::super::tests::execute;
use super::super::*;

#[test]
fn edge_coloring_dispatches_uuid_ordered_parallel_edge_colors() {
    let graph = AdjacencyGraph::with_test_undirected_multigraph(
        4,
        &[(14, 0, 2), (10, 0, 1), (12, 1, 2), (11, 0, 1), (20, 2, 3)],
    );
    let output = execute(
        &graph,
        AnalyzeAlgorithm::EdgeColoring,
        false,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();

    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::EdgeColoring).result_schema()
    );
    assert_eq!(
        output.rows(),
        [(10_u64, 0_u64), (11, 1), (12, 2), (14, 3), (20, 0),]
            .into_iter()
            .map(|(edge, color)| vec![
                AlgorithmValue::Uuid(u128::from(edge).to_be_bytes()),
                AlgorithmValue::UInt64(color),
            ])
            .collect::<Vec<_>>()
    );
}

#[test]
fn edge_coloring_handles_empty_loops_and_shared_controls() {
    assert_eq!(
        execute(
            &AdjacencyGraph::default(),
            AnalyzeAlgorithm::EdgeColoring,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
        .rows(),
        Vec::<Vec<AlgorithmValue>>::new()
    );
    assert!(matches!(
        execute(
            &AdjacencyGraph::with_test_undirected_multigraph(1, &[(10, 0, 0)]),
            AnalyzeAlgorithm::EdgeColoring,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::Execution { message })
            if message == "edge_coloring cannot color a graph containing a self-loop"
    ));
    assert!(matches!(
        execute(
            &AdjacencyGraph::with_test_undirected_multigraph(2, &[(10, 0, 1)]),
            AnalyzeAlgorithm::EdgeColoring,
            false,
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute(
            &AdjacencyGraph::default(),
            AnalyzeAlgorithm::EdgeColoring,
            false,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
fn chromatic_number_dispatches_exact_scalar_for_representative_graphs() {
    for (graph, expected) in [
        (AdjacencyGraph::default(), 0),
        (AdjacencyGraph::with_test_edges(3, &[]), 1),
        (
            AdjacencyGraph::with_test_edges(5, &[(0, 1), (1, 2), (2, 3), (3, 4), (4, 0)]),
            3,
        ),
        (
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]),
            4,
        ),
    ] {
        let output = execute(
            &graph,
            AnalyzeAlgorithm::ChromaticNumber,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::ChromaticNumber).result_schema()
        );
        assert_eq!(output.rows(), [vec![AlgorithmValue::UInt64(expected)]]);
    }
}

#[test]
fn chromatic_number_preserves_loop_failure_and_shared_controls() {
    assert!(matches!(
        execute(
            &AdjacencyGraph::with_test_edges(1, &[(0, 0)]),
            AnalyzeAlgorithm::ChromaticNumber,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::Execution { message })
            if message.contains("undefined for a graph containing a self-loop")
    ));
    assert!(matches!(
        execute(
            &AdjacencyGraph::with_test_edges(2, &[(0, 1)]),
            AnalyzeAlgorithm::ChromaticNumber,
            false,
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute(
            &AdjacencyGraph::default(),
            AnalyzeAlgorithm::ChromaticNumber,
            false,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
fn node_coloring_dispatches_uuid_colors_with_shared_controls() {
    let graph = AdjacencyGraph::with_test_undirected_multigraph(
        5,
        &[
            (10, 0, 1),
            (11, 0, 2),
            (12, 1, 2),
            (13, 2, 3),
            (14, 0, 1),
            (15, 1, 0),
        ],
    );
    let output = execute(
        &graph,
        AnalyzeAlgorithm::NodeColoring,
        false,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::NodeColoring).result_schema()
    );
    assert_eq!(
        output.rows(),
        [
            vec![
                AlgorithmValue::Uuid(0_u128.to_be_bytes()),
                AlgorithmValue::UInt64(0),
            ],
            vec![
                AlgorithmValue::Uuid(1_u128.to_be_bytes()),
                AlgorithmValue::UInt64(1),
            ],
            vec![
                AlgorithmValue::Uuid(2_u128.to_be_bytes()),
                AlgorithmValue::UInt64(2),
            ],
            vec![
                AlgorithmValue::Uuid(3_u128.to_be_bytes()),
                AlgorithmValue::UInt64(0),
            ],
            vec![
                AlgorithmValue::Uuid(4_u128.to_be_bytes()),
                AlgorithmValue::UInt64(0),
            ],
        ]
    );

    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::NodeColoring,
            false,
            AlgorithmLimits {
                output_rows: 4,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::NodeColoring,
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
            AnalyzeAlgorithm::NodeColoring,
            false,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
fn k1_coloring_dispatches_dedicated_uuid_ordered_rows_and_schema() {
    let graph = AdjacencyGraph::with_test_undirected_multigraph(
        5,
        &[(10, 0, 3), (11, 1, 2), (12, 2, 3), (13, 3, 2), (14, 0, 3)],
    );
    let output = execute(
        &graph,
        AnalyzeAlgorithm::K1Coloring,
        false,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::K1Coloring).result_schema()
    );
    assert_eq!(
        output.rows(),
        [0_u64, 1, 0, 1, 0]
            .into_iter()
            .enumerate()
            .map(|(node, color)| vec![
                AlgorithmValue::Uuid(u128::try_from(node).unwrap().to_be_bytes()),
                AlgorithmValue::UInt64(color),
            ])
            .collect::<Vec<_>>()
    );

    let legacy = execute(
        &graph,
        AnalyzeAlgorithm::NodeColoring,
        false,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_ne!(output.rows(), legacy.rows());
    assert_ne!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::ChromaticNumber).result_schema()
    );

    let batch =
        shape_algorithm_output(Algorithm::Analyze(AnalyzeAlgorithm::K1Coloring), &output).unwrap();
    assert_eq!(batch.num_rows(), 5);
    assert_eq!(batch.schema().field(0).name(), "node_uuid");
    assert!(!batch.schema().field(0).is_nullable());
    assert_eq!(batch.schema().field(1).name(), "color");
    assert!(!batch.schema().field(1).is_nullable());
    assert_eq!(
        batch.schema().metadata().get("graphforge.algorithm"),
        Some(&"k1_coloring".to_owned())
    );
    assert_eq!(
        batch
            .schema()
            .metadata()
            .get("graphforge.algorithm_schema_version"),
        Some(&"1".to_owned())
    );
}

#[test]
fn k1_coloring_handler_preserves_empty_loop_cancellation_and_limits() {
    assert!(
        execute(
            &AdjacencyGraph::default(),
            AnalyzeAlgorithm::K1Coloring,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
        .rows()
        .is_empty()
    );
    assert!(matches!(
        execute(
            &AdjacencyGraph::with_test_undirected_multigraph(1, &[(10, 0, 0)]),
            AnalyzeAlgorithm::K1Coloring,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::Execution { message })
            if message == "k1_coloring cannot color a graph containing a self-loop"
    ));
    let graph = AdjacencyGraph::with_test_undirected_multigraph(3, &[(10, 0, 1), (11, 0, 1)]);
    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::K1Coloring,
            false,
            AlgorithmLimits {
                nodes: 2,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::NodeLimit { .. })
    ));
    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::K1Coloring,
            false,
            AlgorithmLimits {
                output_rows: 2,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::K1Coloring,
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
            AnalyzeAlgorithm::K1Coloring,
            false,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
fn k1_coloring_rejects_directed_weight_and_unrelated_options() {
    let dir = tempfile::tempdir().unwrap();
    let provider =
        crate::ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
    for (options, expected) in [
        (
            AnalyzeOptions {
                by: AnalyzeAlgorithm::K1Coloring,
                directed: true,
                ..AnalyzeOptions::default()
            },
            "k1_coloring requires directed=false",
        ),
        (
            AnalyzeOptions {
                by: AnalyzeAlgorithm::K1Coloring,
                directed: false,
                weight: Some("cost".into()),
                ..AnalyzeOptions::default()
            },
            "k1_coloring does not accept an edge weight property",
        ),
    ] {
        assert!(matches!(
            analyze_algorithm(
                &provider,
                dir.path(),
                OntologyMode::Strict,
                EntityTypeSelection::All,
                &options
            ),
            Err(GfError::Validation(message)) if message == expected
        ));
    }
    for options in [
        AnalyzeOptions {
            by: AnalyzeAlgorithm::K1Coloring,
            directed: false,
            k: Some(2),
            ..AnalyzeOptions::default()
        },
        AnalyzeOptions {
            by: AnalyzeAlgorithm::K1Coloring,
            directed: false,
            partition_property: Some("partition".into()),
            ..AnalyzeOptions::default()
        },
    ] {
        assert!(matches!(
            normalize_analyze_options(&options),
            Err(GfError::Validation(_))
        ));
    }
}
