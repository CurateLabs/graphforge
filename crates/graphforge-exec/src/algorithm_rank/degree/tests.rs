use super::super::*;
use super::*;

fn execute_degree(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
) -> Result<AlgorithmOutput, AlgorithmError> {
    execute_rank_with_compute(graph, Algorithm::Rank(RankAlgorithm::Degree), limits, None)
}

fn execute_degree_with_pool(
    graph: &AdjacencyGraph,
    threads: usize,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let pool = Arc::new(crate::ComputePool::new(threads).unwrap());
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry)?;
    let control = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(threads),
        cancellation,
    )
    .with_compute_pool(pool);
    registry.execute(Algorithm::Rank(RankAlgorithm::Degree), graph, &control)
}

fn degree_bits(output: &AlgorithmOutput) -> Vec<u64> {
    output
        .rows()
        .iter()
        .map(|row| match row[1] {
            AlgorithmValue::Float64(score) => score.to_bits(),
            _ => panic!("degree score must be Float64"),
        })
        .collect()
}

fn degree_parallel_graph(nodes: usize) -> AdjacencyGraph {
    let edges = (0..nodes)
        .map(|node| (node as u64, ((node + 1) % nodes) as u64))
        .collect::<Vec<_>>();
    AdjacencyGraph::with_test_edges(nodes as u64, &edges)
}

#[test]
fn degree_scores_a_hand_verifiable_fixture_in_stable_uuid_order() {
    let output = execute_degree(
        &AdjacencyGraph::with_test_counts(3, 4),
        AlgorithmLimits::default(),
    )
    .unwrap();
    assert_eq!(
        output.rows(),
        vec![
            vec![AlgorithmValue::Uuid([0; 16]), AlgorithmValue::Float64(2.0)],
            vec![
                AlgorithmValue::Uuid(u128::from(1_u64).to_be_bytes()),
                AlgorithmValue::Float64(0.0),
            ],
            vec![
                AlgorithmValue::Uuid(u128::from(2_u64).to_be_bytes()),
                AlgorithmValue::Float64(0.0),
            ],
        ]
    );
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry).unwrap();
    assert_eq!(registry.capabilities()[0].dependency, BUILTIN_REVIEW);
}

#[test]
fn degree_handles_empty_graphs_and_shared_resource_limits() {
    assert!(
        execute_degree(&AdjacencyGraph::default(), AlgorithmLimits::default())
            .unwrap()
            .rows()
            .is_empty()
    );
    let limits = AlgorithmLimits {
        nodes: 2,
        ..AlgorithmLimits::default()
    };
    assert_eq!(
        execute_degree(&AdjacencyGraph::with_test_counts(3, 0), limits),
        Err(AlgorithmError::NodeLimit {
            observed: 3,
            limit: 2,
        })
    );
}

#[test]
fn degree_path_selection_respects_threads_crossover_and_pool() {
    let serial_control = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    );
    assert_eq!(
        select_degree_path(&serial_control, DEGREE_PARALLEL_CROSSOVER_NODES - 1),
        DegreeExecutionPath::Serial
    );

    let one = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(1),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
    assert_eq!(
        select_degree_path(&one, DEGREE_PARALLEL_CROSSOVER_NODES),
        DegreeExecutionPath::Serial
    );

    let parallel = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
    assert!(matches!(
        select_degree_path(&parallel, DEGREE_PARALLEL_CROSSOVER_NODES),
        DegreeExecutionPath::Parallel { threads: 4, chunks }
        if chunks > 1
    ));
}

#[test]
fn degree_parallel_matches_one_thread_bits_at_supported_thread_counts() {
    let graph = degree_parallel_graph(DEGREE_PARALLEL_CROSSOVER_NODES);
    let oracle = degree_bits(
        &execute_degree_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap(),
    );
    for threads in [2_usize, 4, 8] {
        let actual = degree_bits(
            &execute_degree_with_pool(&graph, threads, AlgorithmCancellation::default()).unwrap(),
        );
        assert_eq!(actual, oracle, "threads={threads}");
    }
}

#[test]
fn degree_parallel_path_honors_cancellation() {
    let graph = degree_parallel_graph(DEGREE_PARALLEL_CROSSOVER_NODES);
    let cancel = AlgorithmCancellation::default();
    cancel.cancel();
    let err = execute_degree_with_pool(&graph, 4, cancel).unwrap_err();
    assert_eq!(err, AlgorithmError::Cancelled);
}
