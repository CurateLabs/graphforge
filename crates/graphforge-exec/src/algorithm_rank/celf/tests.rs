use super::super::tests::assert_scores_close;
use super::super::tests::hits_hub_scores;
use super::super::*;

fn execute_celf(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry)?;
    registry.execute(
        Algorithm::Rank(RankAlgorithm::Celf),
        graph,
        &AlgorithmControl::new(limits, cancellation),
    )
}

fn celf_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
    hits_hub_scores(output)
}

#[test]
fn celf_scores_edgeless_graphs_deterministically() {
    let graph = AdjacencyGraph::with_test_counts(3, 0);
    let first = execute_celf(
        &graph,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_scores_close(&celf_output_scores(&first), &[1.0, 1.0, 1.0]);
}

#[test]
fn celf_handles_direction_multigraph_self_loop_and_empty_graphs() {
    let single = celf_output_scores(
        &execute_celf(
            &AdjacencyGraph::with_test_edges(3, &[(0, 1)]),
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap(),
    );
    assert!(single[0] > 1.0 && single[1] < 1.0 && (single[2] - 1.0).abs() <= 1.0e-12);
    let parallel = celf_output_scores(
        &execute_celf(
            &AdjacencyGraph::with_test_edges(2, &[(0, 1), (0, 1)]),
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap(),
    );
    assert!(parallel[0] > single[0]);
    assert_scores_close(
        &celf_output_scores(
            &execute_celf(
                &AdjacencyGraph::with_test_edges(1, &[(0, 0)]),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        ),
        &[1.0],
    );
    let directed_chain = celf_output_scores(
        &execute_celf(
            &AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]),
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap(),
    );
    let symmetric_chain = celf_output_scores(
        &execute_celf(
            &AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (1, 2), (2, 1)]),
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap(),
    );
    assert_ne!(symmetric_chain, directed_chain);
}

#[test]
fn celf_uses_shared_limits_cancellation_and_dependency_metadata() {
    let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
    assert!(matches!(
        execute_celf(
            &graph,
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default()
        ),
        Err(AlgorithmError::IterationLimit { .. })
    ));
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute_celf(&graph, AlgorithmLimits::default(), cancellation),
        Err(AlgorithmError::Cancelled)
    );
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry).unwrap();
    let capability = registry
        .capabilities()
        .into_iter()
        .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::Celf))
        .unwrap();
    assert_eq!(capability.dependency, BUILTIN_REVIEW);
}
