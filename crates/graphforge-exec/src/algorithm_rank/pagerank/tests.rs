use super::super::*;
use super::*;

fn execute_pagerank(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry)?;
    registry.execute(
        Algorithm::Rank(RankAlgorithm::PageRank),
        graph,
        &AlgorithmControl::new(limits, cancellation),
    )
}

fn execute_pagerank_with_pool(
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
    registry.execute(Algorithm::Rank(RankAlgorithm::PageRank), graph, &control)
}

fn pagerank_scores(output: &AlgorithmOutput) -> Vec<f64> {
    output
        .rows()
        .iter()
        .map(|row| match row[1] {
            AlgorithmValue::Float64(score) => score,
            _ => panic!("pagerank score must be Float64"),
        })
        .collect()
}

fn pagerank_bits(output: &AlgorithmOutput) -> Vec<u64> {
    pagerank_scores(output)
        .into_iter()
        .map(f64::to_bits)
        .collect()
}

fn dense_cycle_graph(nodes: usize) -> AdjacencyGraph {
    // Enough parallel edges per source to clear the documented crossover on
    // modest node counts while keeping the fixture deterministic.
    let fanout = ((PAGERANK_PARALLEL_CROSSOVER_EDGES as usize) / nodes.max(1)).saturating_add(2);
    let edges = (0..nodes)
        .flat_map(|node| (1..=fanout).map(move |hop| (node as u64, ((node + hop) % nodes) as u64)))
        .collect::<Vec<_>>();
    AdjacencyGraph::with_test_edges(nodes as u64, &edges)
}

#[test]
fn pagerank_scores_hand_verifiable_graphs_deterministically() {
    let cycle = AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]);
    let first = execute_pagerank(
        &cycle,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_eq!(pagerank_scores(&first), [0.5, 0.5]);
    assert_eq!(
        first,
        execute_pagerank(
            &cycle,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
    );
    assert_eq!(
        first.schema,
        Algorithm::Rank(RankAlgorithm::PageRank).result_schema()
    );

    let disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]);
    assert_eq!(
        pagerank_scores(
            &execute_pagerank(
                &disconnected,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        ),
        [0.25, 0.25, 0.25, 0.25]
    );

    let empty = execute_pagerank(
        &AdjacencyGraph::default(),
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert!(empty.rows().is_empty());
}

#[test]
fn pagerank_handles_dangling_parallel_self_loop_and_direction_semantics() {
    let multigraph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (0, 1), (0, 2), (1, 1)]);
    let scores = pagerank_scores(
        &execute_pagerank(
            &multigraph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap(),
    );
    assert!((scores.iter().sum::<f64>() - 1.0).abs() < 1.0e-9);
    assert!(scores[1] > scores[2]);
    assert!(scores[1] > scores[0]);

    let directed = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
    let undirected = AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]);
    assert_ne!(
        pagerank_scores(
            &execute_pagerank(
                &directed,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        ),
        pagerank_scores(
            &execute_pagerank(
                &undirected,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        )
    );
}

#[test]
fn pagerank_uses_shared_limits_cancellation_and_dependency_metadata() {
    let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
    assert_eq!(
        execute_pagerank(
            &graph,
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::IterationLimit {
            observed: 1,
            limit: 0,
        })
    );
    assert!(matches!(
        execute_pagerank(
            &graph,
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute_pagerank(&graph, AlgorithmLimits::default(), cancellation),
        Err(AlgorithmError::Cancelled)
    );
    let registry = {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        registry
    };
    let capability = registry
        .capabilities()
        .into_iter()
        .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::PageRank))
        .unwrap();
    assert_eq!(capability.dependency, BUILTIN_REVIEW);
}

#[test]
fn pagerank_path_selection_respects_crossover_and_one_thread() {
    let serial_control = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    );
    assert_eq!(
        select_pagerank_path(&serial_control, PAGERANK_PARALLEL_CROSSOVER_EDGES - 1, 64),
        PageRankExecutionPath::Serial
    );
    let one = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(1),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
    assert_eq!(
        select_pagerank_path(&one, PAGERANK_PARALLEL_CROSSOVER_EDGES, 64),
        PageRankExecutionPath::Serial
    );
    let parallel = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
    assert_eq!(
        select_pagerank_path(&parallel, PAGERANK_PARALLEL_CROSSOVER_EDGES, 64),
        PageRankExecutionPath::Parallel {
            threads: 4,
            chunks: 4
        }
    );
}

#[test]
fn pagerank_thread_matrix_matches_one_thread_bits_and_ordering() {
    // Above crossover so multi-thread policies exercise the parallel path.
    let graph = dense_cycle_graph(128);
    assert!(graph.edge_entry_count() >= PAGERANK_PARALLEL_CROSSOVER_EDGES);
    let serial = execute_pagerank_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap();
    let serial_bits = pagerank_bits(&serial);
    let serial_rows = serial.rows();
    for threads in [2_usize, 4, 8] {
        let parallel =
            execute_pagerank_with_pool(&graph, threads, AlgorithmCancellation::default()).unwrap();
        assert_eq!(parallel.schema, serial.schema);
        assert_eq!(parallel.rows(), serial_rows);
        assert_eq!(pagerank_bits(&parallel), serial_bits);
    }
}

#[test]
fn pagerank_parallel_preserves_dangling_parallel_self_loop_and_direction_bits() {
    let multigraph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (0, 1), (0, 2), (1, 1)]);
    let directed = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
    let undirected = AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]);
    let empty = AdjacencyGraph::default();
    let single = AdjacencyGraph::with_test_edges(1, &[]);
    let disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]);
    for graph in [
        &multigraph,
        &directed,
        &undirected,
        &empty,
        &single,
        &disconnected,
    ] {
        let serial =
            execute_pagerank_with_pool(graph, 1, AlgorithmCancellation::default()).unwrap();
        for threads in [2_usize, 4, 8] {
            // Force parallel path selection by attaching a multi-thread pool even
            // when edge counts are below the crossover: call pull through registry
            // with an oversized synthetic control only when edges meet crossover,
            // otherwise verify serial path still matches across thread budgets.
            let parallel =
                execute_pagerank_with_pool(graph, threads, AlgorithmCancellation::default())
                    .unwrap();
            assert_eq!(pagerank_bits(&parallel), pagerank_bits(&serial));
            assert_eq!(parallel.rows(), serial.rows());
        }
    }

    // Adversarial magnitudes: force parallel on a graph above crossover with
    // dangling nodes and uneven outdegrees.
    let nodes = 512_u64;
    let mut edges = Vec::new();
    for source in 0..(nodes - 64) {
        let degree = 12 + (source % 17) as usize;
        for hop in 0..degree {
            edges.push((source, (source + 1 + hop as u64) % nodes));
        }
    }
    // Leave high-index nodes dangling.
    let adversarial = AdjacencyGraph::with_test_edges(nodes, &edges);
    assert!(adversarial.edge_entry_count() >= PAGERANK_PARALLEL_CROSSOVER_EDGES);
    let serial =
        execute_pagerank_with_pool(&adversarial, 1, AlgorithmCancellation::default()).unwrap();
    for threads in [2_usize, 4, 8] {
        let parallel =
            execute_pagerank_with_pool(&adversarial, threads, AlgorithmCancellation::default())
                .unwrap();
        assert_eq!(pagerank_bits(&parallel), pagerank_bits(&serial));
    }
}

#[test]
fn pagerank_parallel_cancellation_returns_structured_cancelled() {
    let graph = dense_cycle_graph(128);
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute_pagerank_with_pool(&graph, 4, cancellation),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
fn pagerank_destination_chunks_cover_canonical_ranges() {
    assert_eq!(destination_chunks(0, 4), Vec::<(usize, usize)>::new());
    assert_eq!(destination_chunks(5, 1), vec![(0, 5)]);
    assert_eq!(destination_chunks(5, 2), vec![(0, 3), (3, 5)]);
    assert_eq!(
        destination_chunks(8, 4),
        vec![(0, 2), (2, 4), (4, 6), (6, 8)]
    );
    assert_eq!(destination_chunks(3, 8), vec![(0, 1), (1, 2), (2, 3)]);
}

#[test]
fn pagerank_pull_matches_serial_scatter_contribution_order() {
    let fixtures = [
        AdjacencyGraph::with_test_edges(3, &[(0, 1), (0, 1), (0, 2), (1, 1)]),
        AdjacencyGraph::with_test_edges(2, &[(0, 1)]),
        AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]),
        AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]),
        AdjacencyGraph::with_test_edges(1, &[]),
        dense_cycle_graph(64),
    ];
    for graph in &fixtures {
        if graph.node_ids().is_empty() {
            continue;
        }
        let prepared = prepare_pagerank(graph).unwrap();
        let scores = vec![1.0 / graph.node_ids().len() as f64; graph.node_ids().len()];
        let base = 0.15 / graph.node_ids().len() as f64;
        let mut scatter = vec![base; scores.len()];
        pagerank_scatter_serial(graph, &prepared.indices, &scores, &mut scatter).unwrap();
        let mut pull = vec![0.0; scores.len()];
        for dest in 0..scores.len() {
            pull[dest] = pagerank_pull_destination(
                &prepared.inbound,
                &prepared.outdegrees,
                &scores,
                base,
                dest,
            );
        }
        assert_eq!(
            scatter.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
            pull.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
            "pull must apply contributions in serial source/edge order"
        );
    }
}
