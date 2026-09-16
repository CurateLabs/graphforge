use super::super::tests::assert_scores_close;
use super::super::tests::hits_hub_scores;
use super::super::*;

fn execute_k_core(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry)?;
    registry.execute(
        Algorithm::Rank(RankAlgorithm::KCore),
        graph,
        &AlgorithmControl::new(limits, cancellation),
    )
}

fn k_core_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
    hits_hub_scores(output)
}

#[test]
fn k_core_peels_hand_verifiable_disconnected_layers() {
    let graph = AdjacencyGraph::with_test_edges(
        10,
        &[
            (0, 1),
            (0, 2),
            (0, 3),
            (1, 2),
            (1, 3),
            (2, 3),
            (0, 4),
            (4, 5),
            (7, 8),
            (8, 9),
            (9, 7),
        ],
    );
    let output = execute_k_core(
        &graph,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_scores_close(
        &k_core_output_scores(&output),
        &[3.0, 3.0, 3.0, 3.0, 1.0, 1.0, 0.0, 2.0, 2.0, 2.0],
    );
    assert_eq!(
        output,
        execute_k_core(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
    );
}

#[test]
fn k_core_ignores_direction_multiplicity_and_self_loops() {
    let directed =
        AdjacencyGraph::with_test_edges(4, &[(0, 1), (0, 1), (1, 0), (1, 2), (2, 0), (0, 0)]);
    let reciprocal =
        AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (1, 2), (2, 1), (2, 0), (0, 2)]);
    for graph in [&directed, &reciprocal] {
        assert_scores_close(
            &k_core_output_scores(
                &execute_k_core(
                    graph,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[2.0, 2.0, 2.0, 0.0],
        );
    }
    assert!(
        execute_k_core(
            &AdjacencyGraph::default(),
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
        .rows()
        .is_empty()
    );
}

#[test]
fn k_core_uses_shared_controls_and_dependency_metadata() {
    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2), (2, 0)]);
    assert!(matches!(
        execute_k_core(
            &graph,
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
        execute_k_core(&graph, AlgorithmLimits::default(), cancellation),
        Err(AlgorithmError::Cancelled)
    );
    assert!(matches!(
        execute_k_core(
            &graph,
            AlgorithmLimits {
                output_rows: 2,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit {
            observed: 3,
            limit: 2
        })
    ));
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry).unwrap();
    let capability = registry
        .capabilities()
        .into_iter()
        .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::KCore))
        .unwrap();
    assert_eq!(capability.dependency, BUILTIN_REVIEW);
    assert_eq!(capability.algorithm.as_str(), "k_core");
}

#[test]
fn k_core_serial_disposition_matches_across_compute_thread_budgets() {
    let graph = AdjacencyGraph::with_test_edges(
        10,
        &[
            (0, 1),
            (0, 2),
            (0, 3),
            (1, 2),
            (1, 3),
            (2, 3),
            (0, 4),
            (4, 5),
            (7, 8),
            (8, 9),
            (9, 7),
        ],
    );
    let limits = AlgorithmLimits {
        batch_size: 2,
        ..AlgorithmLimits::default()
    };
    let algorithm = Algorithm::Rank(RankAlgorithm::KCore);
    let oracle = execute_rank_with_compute(
        &graph,
        algorithm,
        limits.with_compute_threads(1),
        Some(Arc::new(crate::ComputePool::new(1).unwrap())),
    )
    .unwrap();
    assert_eq!(oracle.peak_builder_rows, 2);
    assert_eq!(oracle.internal_batch_count, 5);

    for threads in [2, 4, 8] {
        let candidate = execute_rank_with_compute(
            &graph,
            algorithm,
            limits.with_compute_threads(threads),
            Some(Arc::new(crate::ComputePool::new(threads).unwrap())),
        )
        .unwrap();
        assert_eq!(
            candidate, oracle,
            "serial k-core disposition must match one-thread oracle at {threads} threads"
        );
    }
}

#[test]
fn k_core_batches_heap_entry_checkpoints() {
    let edges: Vec<(u64, u64)> = (1..=6_000).map(|leaf| (leaf, 0)).collect();
    let output = execute_k_core(
        &AdjacencyGraph::with_test_edges(6_001, &edges),
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert!(
        k_core_output_scores(&output)
            .into_iter()
            .all(|score| score == 1.0)
    );
}
