use super::super::tests::execute;
use super::super::*;

fn execute_with_compute_threads(
    graph: &AdjacencyGraph,
    algorithm: AnalyzeAlgorithm,
    directed: bool,
    threads: usize,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    register_analyze_algorithms(&mut registry, directed)?;
    let control = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(threads),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(threads).unwrap()));
    registry.execute(Algorithm::Analyze(algorithm), graph, &control)
}

fn output_fingerprint(output: &AlgorithmOutput) -> String {
    format!("{:?}|{:?}", output.schema, output.rows())
}

fn execute_automorphism_count(
    graph: &AdjacencyGraph,
    directed: bool,
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    execute(
        graph,
        AnalyzeAlgorithm::CountAutomorphisms,
        directed,
        limits,
        cancellation,
    )
}

fn automorphism_count(graph: &AdjacencyGraph, directed: bool) -> u64 {
    let output = execute_automorphism_count(
        graph,
        directed,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::CountAutomorphisms).result_schema()
    );
    let rows = output.rows();
    let [row] = rows.as_slice() else {
        panic!("automorphism dispatch must return exactly one row");
    };
    let [AlgorithmValue::UInt64(count)] = row.as_slice() else {
        panic!("automorphism dispatch must return one UInt64 value");
    };
    *count
}

#[test]
fn automorphism_dispatch_counts_canonical_graph_families_and_schema() {
    assert_eq!(
        automorphism_count(&AdjacencyGraph::with_test_edges(0, &[]), false),
        1
    );
    assert_eq!(
        automorphism_count(&AdjacencyGraph::with_test_edges(1, &[]), false),
        1
    );
    assert_eq!(
        automorphism_count(
            &AdjacencyGraph::with_test_undirected_multigraph(3, &[(10, 0, 1), (11, 1, 2)],),
            false,
        ),
        2
    );
    assert_eq!(
        automorphism_count(
            &AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2), (2, 0)]),
            true,
        ),
        3
    );
    assert_eq!(
        automorphism_count(
            &AdjacencyGraph::with_test_undirected_multigraph(2, &[(10, 0, 0), (11, 0, 1)],),
            false,
        ),
        1
    );
    assert_eq!(
        automorphism_count(
            &AdjacencyGraph::with_test_undirected_multigraph(
                3,
                &[(10, 0, 1), (11, 0, 1), (12, 0, 2)],
            ),
            false,
        ),
        1
    );
}

#[test]
fn automorphism_dispatch_is_repeatable_uuid_rename_invariant_and_registered() {
    let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2), (2, 0)]);
    let renamed = AdjacencyGraph::with_test_directed_edges_and_uuids(
        &[
            90_u128.to_be_bytes(),
            2_u128.to_be_bytes(),
            70_u128.to_be_bytes(),
        ],
        &[(0, 1), (1, 2), (2, 0)],
    );
    assert_eq!(automorphism_count(&graph, true), 3);
    assert_eq!(
        automorphism_count(&graph, true),
        automorphism_count(&renamed, true)
    );

    let mut registry = AlgorithmRegistry::default();
    register_analyze_algorithms(&mut registry, true).unwrap();
    assert_eq!(
        registry
            .capabilities()
            .into_iter()
            .filter(|capability| {
                capability.algorithm == Algorithm::Analyze(AnalyzeAlgorithm::CountAutomorphisms)
            })
            .count(),
        1
    );
}

#[test]
fn automorphism_dispatch_propagates_cancellation_and_resource_limits_atomically() {
    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute_automorphism_count(&graph, false, AlgorithmLimits::default(), cancellation,),
        Err(AlgorithmError::Cancelled)
    );
    assert_eq!(
        execute_automorphism_count(
            &graph,
            false,
            AlgorithmLimits {
                nodes: 2,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::NodeLimit {
            observed: 3,
            limit: 2,
        })
    );
    assert!(matches!(
        execute_automorphism_count(
            &graph,
            false,
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::IterationLimit { .. })
    ));
    assert_eq!(
        execute_automorphism_count(
            &graph,
            false,
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit {
            observed: 1,
            limit: 0,
        })
    );
}

#[test]
fn triangle_count_dispatches_exact_scalar_and_shared_controls() {
    let graph = AdjacencyGraph::with_test_undirected_multigraph(
        6,
        &[
            (10, 0, 1),
            (11, 1, 2),
            (12, 2, 0),
            (13, 0, 1),
            (14, 1, 0),
            (15, 1, 3),
            (16, 2, 3),
            (17, 4, 4),
        ],
    );
    let output = execute(
        &graph,
        AnalyzeAlgorithm::TriangleCount,
        false,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::TriangleCount).result_schema()
    );
    assert_eq!(output.rows(), [vec![AlgorithmValue::UInt64(2)]]);

    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::TriangleCount,
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
            &graph,
            AnalyzeAlgorithm::TriangleCount,
            false,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
fn triad_census_dispatches_canonical_rows_and_shared_controls() {
    let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2), (2, 0)]);
    let output = execute(
        &graph,
        AnalyzeAlgorithm::TriadCensus,
        true,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::TriadCensus).result_schema()
    );
    assert_eq!(output.rows().len(), 16);
    for (index, name) in TRIAD_NAMES.iter().enumerate() {
        assert_eq!(
            output.rows()[index],
            [
                AlgorithmValue::Utf8((*name).to_owned()),
                AlgorithmValue::UInt64(u64::from(index == 9)),
            ]
        );
    }

    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::TriadCensus,
            true,
            AlgorithmLimits {
                output_rows: 15,
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
            &graph,
            AnalyzeAlgorithm::TriadCensus,
            true,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
fn transitivity_dispatches_exact_scalar_and_shared_controls() {
    let graph = AdjacencyGraph::with_test_undirected_multigraph(
        6,
        &[
            (10, 0, 1),
            (11, 1, 2),
            (12, 2, 0),
            (13, 0, 1),
            (14, 1, 0),
            (15, 1, 3),
            (16, 2, 3),
            (17, 4, 4),
        ],
    );
    let output = execute(
        &graph,
        AnalyzeAlgorithm::Transitivity,
        false,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::Transitivity).result_schema()
    );
    assert_eq!(output.rows(), [vec![AlgorithmValue::Float64(0.75)]]);

    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::Transitivity,
            false,
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::Transitivity,
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
            AnalyzeAlgorithm::Transitivity,
            false,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
fn is_planar_dispatches_exact_boolean_schema_and_controls() {
    let graph = AdjacencyGraph::with_test_undirected_multigraph(
        6,
        &[
            (10, 0, 3),
            (11, 0, 4),
            (12, 0, 5),
            (13, 1, 3),
            (14, 1, 4),
            (15, 1, 5),
            (16, 2, 3),
            (17, 2, 4),
            (18, 2, 5),
            (19, 0, 3),
            (20, 0, 0),
        ],
    );
    let output = execute(
        &graph,
        AnalyzeAlgorithm::IsPlanar,
        false,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::IsPlanar).result_schema()
    );
    assert_eq!(output.rows(), [vec![AlgorithmValue::Boolean(false)]]);

    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::IsPlanar,
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
            &graph,
            AnalyzeAlgorithm::IsPlanar,
            false,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
fn dyad_census_dispatches_fixed_order_counts_and_shared_controls() {
    let graph = AdjacencyGraph::with_test_directed_edges(
        5,
        &[(0, 1), (1, 0), (0, 1), (0, 2), (3, 2), (4, 4)],
    );
    let output = execute(
        &graph,
        AnalyzeAlgorithm::DyadCensus,
        true,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::DyadCensus).result_schema()
    );
    assert_eq!(
        output.rows(),
        [
            vec![
                AlgorithmValue::Utf8("mutual".into()),
                AlgorithmValue::UInt64(1),
            ],
            vec![
                AlgorithmValue::Utf8("asymmetric".into()),
                AlgorithmValue::UInt64(2),
            ],
            vec![
                AlgorithmValue::Utf8("null".into()),
                AlgorithmValue::UInt64(7),
            ],
        ]
    );

    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::DyadCensus,
            true,
            AlgorithmLimits {
                output_rows: 2,
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
            &graph,
            AnalyzeAlgorithm::DyadCensus,
            true,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
fn dyad_census_shapes_canonical_arrow_schema_and_metadata() {
    let output = execute(
        &AdjacencyGraph::with_test_directed_edges(2, &[(0, 1)]),
        AnalyzeAlgorithm::DyadCensus,
        true,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    let algorithm = Algorithm::Analyze(AnalyzeAlgorithm::DyadCensus);
    let batch = shape_algorithm_output(algorithm, &output).unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.schema().field(0).name(), "dyad_type");
    assert!(!batch.schema().field(0).is_nullable());
    assert_eq!(batch.schema().field(1).name(), "count");
    assert!(!batch.schema().field(1).is_nullable());
    assert_eq!(
        batch.schema().metadata().get("graphforge.algorithm"),
        Some(&"dyad_census".to_owned())
    );
    assert_eq!(
        batch.schema().metadata().get("graphforge.verb"),
        Some(&"analyze".to_owned())
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
fn articulation_points_dispatches_uuid_rows_with_shared_controls() {
    let graph = AdjacencyGraph::with_test_undirected_multigraph(
        8,
        &[
            (10, 0, 1),
            (11, 1, 2),
            (12, 2, 0),
            (13, 1, 3),
            (14, 3, 1),
            (15, 3, 4),
            (16, 3, 3),
            (17, 5, 6),
        ],
    );
    let output = execute(
        &graph,
        AnalyzeAlgorithm::ArticulationPoints,
        false,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::ArticulationPoints).result_schema()
    );
    assert_eq!(
        output.rows(),
        [
            vec![AlgorithmValue::Uuid(1_u128.to_be_bytes())],
            vec![AlgorithmValue::Uuid(3_u128.to_be_bytes())],
        ]
    );

    for limits in [
        AlgorithmLimits {
            nodes: 7,
            ..AlgorithmLimits::default()
        },
        AlgorithmLimits {
            edges: 14,
            ..AlgorithmLimits::default()
        },
        AlgorithmLimits {
            output_rows: 1,
            ..AlgorithmLimits::default()
        },
    ] {
        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::ArticulationPoints,
                false,
                limits,
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::NodeLimit { .. }
                | AlgorithmError::EdgeLimit { .. }
                | AlgorithmError::OutputLimit { .. })
        ));
    }
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute(
            &graph,
            AnalyzeAlgorithm::ArticulationPoints,
            false,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
fn articulation_points_keeps_serial_fingerprint_under_thread_budgets() {
    let graph = AdjacencyGraph::with_test_undirected_multigraph(
        10,
        &[
            (10, 0, 1),
            (11, 1, 2),
            (12, 2, 0),
            (13, 1, 3),
            (14, 3, 4),
            (15, 4, 5),
            (16, 5, 3),
            (17, 5, 6),
            (18, 6, 7),
            (19, 7, 5),
            (20, 7, 8),
            (21, 8, 9),
            (22, 7, 8),
        ],
    );
    let serial =
        execute_with_compute_threads(&graph, AnalyzeAlgorithm::ArticulationPoints, false, 1)
            .unwrap();
    assert_eq!(
        serial.rows(),
        [
            vec![AlgorithmValue::Uuid(1_u128.to_be_bytes())],
            vec![AlgorithmValue::Uuid(3_u128.to_be_bytes())],
            vec![AlgorithmValue::Uuid(5_u128.to_be_bytes())],
            vec![AlgorithmValue::Uuid(7_u128.to_be_bytes())],
            vec![AlgorithmValue::Uuid(8_u128.to_be_bytes())],
        ]
    );
    let serial_fingerprint = output_fingerprint(&serial);

    for threads in [2_usize, 4, 8] {
        let output = execute_with_compute_threads(
            &graph,
            AnalyzeAlgorithm::ArticulationPoints,
            false,
            threads,
        )
        .unwrap();
        assert_eq!(output.schema, serial.schema);
        assert_eq!(output_fingerprint(&output), serial_fingerprint);
    }
}

#[test]
fn bridges_dispatches_canonical_uuid_rows_with_shared_controls() {
    let graph = AdjacencyGraph::with_test_undirected_multigraph(
        8,
        &[
            (10, 0, 1),
            (11, 1, 2),
            (12, 2, 0),
            (13, 1, 3),
            (14, 3, 1),
            (15, 3, 4),
            (16, 3, 3),
            (17, 5, 6),
        ],
    );
    let output = execute(
        &graph,
        AnalyzeAlgorithm::Bridges,
        false,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::Bridges).result_schema()
    );
    assert_eq!(
        output.rows(),
        [
            vec![
                AlgorithmValue::Uuid(15_u128.to_be_bytes()),
                AlgorithmValue::Uuid(3_u128.to_be_bytes()),
                AlgorithmValue::Uuid(4_u128.to_be_bytes()),
            ],
            vec![
                AlgorithmValue::Uuid(17_u128.to_be_bytes()),
                AlgorithmValue::Uuid(5_u128.to_be_bytes()),
                AlgorithmValue::Uuid(6_u128.to_be_bytes()),
            ],
        ]
    );

    for limits in [
        AlgorithmLimits {
            nodes: 7,
            ..AlgorithmLimits::default()
        },
        AlgorithmLimits {
            edges: 14,
            ..AlgorithmLimits::default()
        },
        AlgorithmLimits {
            output_rows: 1,
            ..AlgorithmLimits::default()
        },
    ] {
        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::Bridges,
                false,
                limits,
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::NodeLimit { .. }
                | AlgorithmError::EdgeLimit { .. }
                | AlgorithmError::OutputLimit { .. })
        ));
    }
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute(
            &graph,
            AnalyzeAlgorithm::Bridges,
            false,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
fn bridges_keeps_serial_fingerprint_under_thread_budgets() {
    let graph = AdjacencyGraph::with_test_undirected_multigraph(
        10,
        &[
            (10, 0, 1),
            (11, 1, 2),
            (12, 2, 0),
            (13, 1, 3),
            (14, 3, 4),
            (15, 4, 5),
            (16, 5, 3),
            (17, 5, 6),
            (18, 6, 7),
            (19, 7, 5),
            (20, 7, 8),
            (21, 8, 9),
            (22, 7, 8),
        ],
    );
    let serial = execute_with_compute_threads(&graph, AnalyzeAlgorithm::Bridges, false, 1).unwrap();
    assert_eq!(
        serial.rows(),
        [
            vec![
                AlgorithmValue::Uuid(13_u128.to_be_bytes()),
                AlgorithmValue::Uuid(1_u128.to_be_bytes()),
                AlgorithmValue::Uuid(3_u128.to_be_bytes()),
            ],
            vec![
                AlgorithmValue::Uuid(21_u128.to_be_bytes()),
                AlgorithmValue::Uuid(8_u128.to_be_bytes()),
                AlgorithmValue::Uuid(9_u128.to_be_bytes()),
            ],
        ]
    );
    let serial_fingerprint = output_fingerprint(&serial);

    for threads in [2_usize, 4, 8] {
        let output =
            execute_with_compute_threads(&graph, AnalyzeAlgorithm::Bridges, false, threads)
                .unwrap();
        assert_eq!(output.schema, serial.schema);
        assert_eq!(output_fingerprint(&output), serial_fingerprint);
    }
}
