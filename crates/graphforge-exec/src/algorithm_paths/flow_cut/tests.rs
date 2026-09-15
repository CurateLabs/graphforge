use super::super::tests::uuid;
use super::super::tests::value;
use super::super::*;
use super::*;

fn execute_gomory_hu(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let algorithm = Algorithm::Paths(PathAlgorithm::GomoryHuTree);
    let mut registry = AlgorithmRegistry::default();
    registry.register(Arc::new(GomoryHuTree))?;
    registry.execute(
        algorithm,
        graph,
        &AlgorithmControl::new(limits, cancellation),
    )
}

fn execute_flow(
    graph: &AdjacencyGraph,
    algorithm: PathAlgorithm,
    source: u64,
    target: u64,
    limits: AlgorithmLimits,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    register_path_algorithms(
        &mut registry,
        uuid(source),
        Some(uuid(target)),
        1,
        None,
        None,
    )?;
    registry.execute(
        Algorithm::Paths(algorithm),
        graph,
        &AlgorithmControl::new(limits, AlgorithmCancellation::default()),
    )
}

fn execute_cut(
    graph: &AdjacencyGraph,
    algorithm: PathAlgorithm,
    source: u64,
    target: u64,
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    register_path_algorithms(
        &mut registry,
        uuid(source),
        Some(uuid(target)),
        1,
        None,
        None,
    )?;
    registry.execute(
        Algorithm::Paths(algorithm),
        graph,
        &AlgorithmControl::new(limits, cancellation),
    )
}

fn execute_min_cost_flow(
    graph: &AdjacencyGraph,
    algorithm: PathAlgorithm,
    limits: AlgorithmLimits,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let input: Arc<[CostCapacityEdge]> = Arc::from([
        CostCapacityEdge {
            edge_uuid: uuid(10),
            source_uuid: uuid(0),
            target_uuid: uuid(1),
            capacity: 2.0,
            unit_cost: -1.0,
        },
        CostCapacityEdge {
            edge_uuid: uuid(11),
            source_uuid: uuid(1),
            target_uuid: uuid(2),
            capacity: 2.0,
            unit_cost: 3.0,
        },
        CostCapacityEdge {
            edge_uuid: uuid(12),
            source_uuid: uuid(0),
            target_uuid: uuid(2),
            capacity: 1.0,
            unit_cost: 5.0,
        },
    ]);
    let mut registry = AlgorithmRegistry::default();
    register_path_algorithms(&mut registry, uuid(0), Some(uuid(2)), 1, None, Some(input))?;
    registry.execute(
        Algorithm::Paths(algorithm),
        graph,
        &AlgorithmControl::new(limits, AlgorithmCancellation::default()),
    )
}

#[test]
fn gomory_hu_dispatch_shapes_canonical_forest_and_honors_controls() {
    for graph in [
        AdjacencyGraph::with_test_counts(0, 0),
        AdjacencyGraph::with_test_counts(1, 0),
    ] {
        assert!(
            execute_gomory_hu(
                &graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }
    let graph =
        AdjacencyGraph::with_test_undirected_multigraph(4, &[(10, 0, 1), (11, 0, 2), (12, 1, 2)])
            .with_test_edge_weights(&[3.0, 3.0, 2.0, 2.0, 4.0, 4.0]);
    let output = execute_gomory_hu(
        &graph,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_eq!(
        output.schema,
        Algorithm::Paths(PathAlgorithm::GomoryHuTree).result_schema()
    );
    assert_eq!(
        output.rows(),
        vec![
            vec![value(0), value(1), AlgorithmValue::Float64(5.0),],
            vec![value(1), value(2), AlgorithmValue::Float64(6.0),],
        ]
    );

    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute_gomory_hu(&graph, AlgorithmLimits::default(), cancellation),
        Err(AlgorithmError::Cancelled)
    );
    assert!(matches!(
        execute_gomory_hu(
            &graph,
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
}

#[test]
fn min_cost_flow_views_share_one_typed_solution_and_limits() {
    let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2), (0, 2)]);
    let scalar = execute_min_cost_flow(
        &graph,
        PathAlgorithm::MinCostMaxFlow,
        AlgorithmLimits::default(),
    )
    .unwrap();
    assert_eq!(
        scalar.rows(),
        vec![vec![
            value(0),
            value(2),
            AlgorithmValue::Float64(3.0),
            AlgorithmValue::Float64(9.0),
        ]]
    );
    assert!(
        execute_min_cost_flow(
            &graph,
            PathAlgorithm::MinCostMaxFlow,
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
        )
        .is_ok()
    );
    let edges = execute_min_cost_flow(
        &graph,
        PathAlgorithm::MinCostMaxFlowEdges,
        AlgorithmLimits::default(),
    )
    .unwrap();
    assert_eq!(edges.rows().len(), 3);
    assert_eq!(
        edges.schema,
        Algorithm::Paths(PathAlgorithm::MinCostMaxFlowEdges).result_schema()
    );
    assert!(matches!(
        execute_min_cost_flow(
            &graph,
            PathAlgorithm::MinCostMaxFlowEdges,
            AlgorithmLimits {
                output_rows: 2,
                ..AlgorithmLimits::default()
            },
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
    assert_eq!(
        execute_min_cost_flow(
            &graph,
            PathAlgorithm::MinCostMaxFlow,
            AlgorithmLimits {
                nodes: 2,
                ..AlgorithmLimits::default()
            },
        ),
        Err(AlgorithmError::NodeLimit {
            observed: 3,
            limit: 2,
        })
    );
}

#[test]
fn min_cost_flow_checks_cancellation_before_node_projection_allocation() {
    let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]);
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    MIN_COST_NODE_PROJECTION_ATTEMPTS.with(|attempts| attempts.set(0));
    let handler = MinCostFlow {
        source: uuid(0),
        target: Some(uuid(2)),
        input: Arc::from([CostCapacityEdge {
            edge_uuid: uuid(10),
            source_uuid: uuid(0),
            target_uuid: uuid(1),
            capacity: 1.0,
            unit_cost: 0.0,
        }]),
        edges: false,
    };

    assert_eq!(
        handler.execute(
            &graph,
            &AlgorithmControl::new(AlgorithmLimits::default(), cancellation,),
        ),
        Err(AlgorithmError::Cancelled)
    );
    MIN_COST_NODE_PROJECTION_ATTEMPTS.with(|attempts| {
        assert_eq!(
            attempts.get(),
            0,
            "cancelled execution reached node projection"
        );
    });
}

#[test]
fn maximum_flow_views_share_one_canonical_solution_and_apply_view_limits() {
    let graph =
        AdjacencyGraph::with_test_directed_edges(4, &[(0, 1), (0, 2), (1, 2), (1, 3), (2, 3)])
            .with_test_edge_weights(&[3.0, 2.0, 1.0, 2.0, 4.0]);
    let scalar = execute_flow(
        &graph,
        PathAlgorithm::MaxFlow,
        0,
        3,
        AlgorithmLimits {
            output_rows: 1,
            ..AlgorithmLimits::default()
        },
    )
    .unwrap();
    let edges = execute_flow(
        &graph,
        PathAlgorithm::MaxFlowEdges,
        0,
        3,
        AlgorithmLimits::default(),
    )
    .unwrap();
    assert_eq!(
        scalar,
        crate::algorithm_output::shape_logical_rows(
            Algorithm::Paths(PathAlgorithm::MaxFlow),
            vec![vec![value(0), value(3), AlgorithmValue::Float64(5.0)]],
            8192,
            u64::MAX
        )
        .unwrap()
    );
    assert_eq!(
        edges.rows().iter().map(|row| &row[3]).collect::<Vec<_>>(),
        vec![
            &AlgorithmValue::Float64(3.0),
            &AlgorithmValue::Float64(2.0),
            &AlgorithmValue::Float64(1.0),
            &AlgorithmValue::Float64(2.0),
            &AlgorithmValue::Float64(3.0),
        ]
    );
    assert!(matches!(
        execute_flow(
            &graph,
            PathAlgorithm::MaxFlowEdges,
            0,
            3,
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
        ),
        Err(AlgorithmError::OutputLimit {
            observed: 2,
            limit: 1
        })
    ));
}

#[test]
fn minimum_cut_views_shape_one_shared_canonical_solution() {
    let graph =
        AdjacencyGraph::with_test_directed_edges(4, &[(0, 1), (0, 2), (1, 2), (1, 3), (2, 3)])
            .with_test_edge_weights(&[3.0, 2.0, 1.0, 2.0, 4.0]);
    let scalar = execute_cut(
        &graph,
        PathAlgorithm::MinCut,
        0,
        3,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    let edges = execute_cut(
        &graph,
        PathAlgorithm::MinCutEdges,
        0,
        3,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();

    assert_eq!(
        scalar,
        crate::algorithm_output::shape_logical_rows(
            Algorithm::Paths(PathAlgorithm::MinCut),
            vec![vec![value(0), value(3), AlgorithmValue::Float64(5.0)]],
            8192,
            u64::MAX
        )
        .unwrap()
    );
    assert_eq!(
        edges,
        crate::algorithm_output::shape_logical_rows(
            Algorithm::Paths(PathAlgorithm::MinCutEdges),
            vec![
                vec![value(0), value(0), value(1), AlgorithmValue::Float64(3.0)],
                vec![value(1), value(0), value(2), AlgorithmValue::Float64(2.0)],
            ],
            8192,
            u64::MAX
        )
        .unwrap()
    );
    assert_eq!(
        scalar.rows()[0][2],
        AlgorithmValue::Float64(
            edges
                .rows()
                .iter()
                .map(|row| match &row[3] {
                    AlgorithmValue::Float64(capacity) => *capacity,
                    _ => unreachable!("minimum-cut edge capacity is Float64"),
                })
                .sum()
        )
    );
    for (algorithm, output, fields) in [
        (
            PathAlgorithm::MinCut,
            &scalar,
            vec!["source_uuid", "sink_uuid", "cut_value"],
        ),
        (
            PathAlgorithm::MinCutEdges,
            &edges,
            vec!["edge_uuid", "source_uuid", "target_uuid", "capacity"],
        ),
    ] {
        let batch = shape_algorithm_output(Algorithm::Paths(algorithm), output).unwrap();
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            fields
        );
        assert!(
            batch
                .schema()
                .fields()
                .iter()
                .all(|field| !field.is_nullable())
        );
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            algorithm.as_str()
        );
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "paths");
    }
}

#[test]
fn minimum_cut_edges_preserve_undirected_storage_orientation_and_zero_results() {
    let undirected =
        AdjacencyGraph::with_test_undirected_multigraph(4, &[(10, 0, 1), (11, 1, 2), (12, 2, 3)])
            .with_test_edge_weights(&[2.0; 6]);
    assert_eq!(
        execute_cut(
            &undirected,
            PathAlgorithm::MinCutEdges,
            3,
            0,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
        .rows(),
        vec![vec![
            value(10),
            value(0),
            value(1),
            AlgorithmValue::Float64(2.0),
        ]]
    );

    let unreachable = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1)]);
    assert_eq!(
        execute_cut(
            &unreachable,
            PathAlgorithm::MinCut,
            0,
            2,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
        .rows(),
        vec![vec![value(0), value(2), AlgorithmValue::Float64(0.0)]]
    );
    assert!(
        execute_cut(
            &unreachable,
            PathAlgorithm::MinCutEdges,
            0,
            2,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
        .rows()
        .is_empty()
    );
}

#[test]
fn minimum_cut_dispatch_rejects_invalid_inputs_and_propagates_controls() {
    let graph = AdjacencyGraph::with_test_directed_edges(2, &[(0, 1)]);
    assert!(matches!(
        execute_cut(
            &graph,
            PathAlgorithm::MinCut,
            0,
            0,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::Execution { message })
            if message == "minimum cut requires distinct endpoints"
    ));
    let invalid =
        AdjacencyGraph::with_test_directed_edges(2, &[(0, 1)]).with_test_edge_weights(&[f64::NAN]);
    assert!(matches!(
        execute_cut(
            &invalid,
            PathAlgorithm::MinCut,
            0,
            1,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::Execution { message })
            if message == "minimum cut requires finite nonnegative capacities"
    ));
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert!(matches!(
        execute_cut(
            &graph,
            PathAlgorithm::MinCut,
            0,
            1,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    ));
    assert!(matches!(
        execute_cut(
            &graph,
            PathAlgorithm::MinCutEdges,
            0,
            1,
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit {
            observed: 1,
            limit: 0
        })
    ));
}

#[test]
fn undirected_flow_rows_use_canonical_endpoints_and_signed_assignments() {
    let graph =
        AdjacencyGraph::with_test_undirected_multigraph(4, &[(10, 1, 0), (11, 2, 1), (12, 3, 2)])
            .with_test_edge_weights(&[2.0, 2.0, 2.0, 2.0, 2.0, 2.0]);
    let rows: Vec<Vec<AlgorithmValue>> = execute_flow(
        &graph,
        PathAlgorithm::MaxFlowEdges,
        3,
        0,
        AlgorithmLimits::default(),
    )
    .unwrap()
    .rows();
    assert_eq!(
        rows,
        vec![
            vec![value(10), value(0), value(1), AlgorithmValue::Float64(-2.0)],
            vec![value(11), value(1), value(2), AlgorithmValue::Float64(-2.0)],
            vec![value(12), value(2), value(3), AlgorithmValue::Float64(-2.0)],
        ]
    );
}

#[test]
fn maximum_flow_views_reject_noncanonical_k() {
    for by in [
        PathAlgorithm::MaxFlow,
        PathAlgorithm::MaxFlowEdges,
        PathAlgorithm::MinCut,
        PathAlgorithm::MinCutEdges,
    ] {
        for k in [0, 2] {
            assert!(matches!(
                validate_path_options(
                    Some(uuid(0)),
                    Some(uuid(1)),
                    &PathsOptions {
                        by,
                        k,
                        ..PathsOptions::default()
                    },
                ),
                Err(GfError::Validation(message))
                    if message == format!("{by} k must be 1")
            ));
        }
    }
}

#[test]
fn minimum_cut_views_require_target_and_reject_unrelated_options() {
    for by in [PathAlgorithm::MinCut, PathAlgorithm::MinCutEdges] {
        assert!(matches!(
            validate_path_options(
                Some(uuid(0)),
                None,
                &PathsOptions {
                    by,
                    ..PathsOptions::default()
                },
            ),
            Err(GfError::Validation(message))
                if message == format!("{by} requires a target selector")
        ));
        assert!(matches!(
            validate_path_options(
                Some(uuid(0)),
                Some(uuid(1)),
                &PathsOptions {
                    by,
                    heuristic: Some("estimate".into()),
                    ..PathsOptions::default()
                },
            ),
            Err(GfError::Validation(message))
                if message == format!("{by} does not accept a heuristic property")
        ));
        assert!(matches!(
            validate_path_options(
                Some(uuid(0)),
                Some(uuid(1)),
                &PathsOptions {
                    by,
                    seed: Some(7),
                    ..PathsOptions::default()
                },
            ),
            Err(GfError::Validation(message))
                if message == format!("{by} does not accept random-walk options")
        ));
    }
}

#[test]
fn min_cost_flow_public_options_require_exact_capacity_and_cost_contract() {
    for by in [
        PathAlgorithm::MinCostMaxFlow,
        PathAlgorithm::MinCostMaxFlowEdges,
    ] {
        let validate =
            |options: PathsOptions| validate_path_options(Some(uuid(0)), Some(uuid(1)), &options);
        assert!(matches!(
            validate(PathsOptions {
                by,
                weight: Some("weight".into()),
                capacity_property: Some("capacity".into()),
                cost_property: Some("cost".into()),
                ..PathsOptions::default()
            }),
            Err(GfError::Validation(message))
                if message == format!(
                    "{by} uses capacity_property and cost_property instead of weight"
                )
        ));
        assert!(matches!(
            validate(PathsOptions {
                by,
                capacity_property: Some("capacity".into()),
                ..PathsOptions::default()
            }),
            Err(GfError::Validation(message))
                if message == format!("{by} requires a cost_property")
        ));
        assert!(matches!(
            validate(PathsOptions {
                by,
                capacity_property: Some(" bad".into()),
                cost_property: Some("cost".into()),
                ..PathsOptions::default()
            }),
            Err(GfError::Validation(message))
                if message == "invalid paths capacity property \" bad\""
        ));
        assert!(
            validate(PathsOptions {
                by,
                capacity_property: Some("capacity".into()),
                cost_property: Some("cost".into()),
                ..PathsOptions::default()
            })
            .is_ok()
        );
    }

    assert!(matches!(
        validate_path_options(
            Some(uuid(0)),
            Some(uuid(1)),
            &PathsOptions {
                by: PathAlgorithm::MaxFlow,
                capacity_property: Some("capacity".into()),
                cost_property: Some("cost".into()),
                ..PathsOptions::default()
            }
        ),
        Err(GfError::Validation(message))
            if message == "max_flow does not accept min-cost flow properties"
    ));
}

#[test]
fn gomory_hu_public_validation_rejects_positional_and_directed_requests() {
    let positional = PathsOptions {
        by: PathAlgorithm::GomoryHuTree,
        ..PathsOptions::default()
    };
    assert!(matches!(
        validate_path_options(Some(uuid(0)), None, &positional),
        Err(GfError::Validation(message))
            if message.contains("does not accept positional source or target")
    ));

    let directed = PathsOptions {
        by: PathAlgorithm::GomoryHuTree,
        directed: true,
        ..PathsOptions::default()
    };
    assert!(matches!(
        validate_path_options(None, None, &directed),
        Err(GfError::Validation(message)) if message == "gomory_hu_tree requires directed=false"
    ));
}
