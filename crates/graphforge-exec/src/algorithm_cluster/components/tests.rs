use super::super::tests::community_ids;
use super::super::*;
use super::*;

fn execute_components(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
) -> Result<AlgorithmOutput, AlgorithmError> {
    execute_cluster(
        graph,
        Algorithm::Cluster(ClusterAlgorithm::Components),
        limits,
    )
}

fn execute_components_with_pool(
    graph: &AdjacencyGraph,
    threads: usize,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let pool = Arc::new(crate::ComputePool::new(threads).unwrap());
    let mut registry = AlgorithmRegistry::default();
    register_cluster_algorithms(&mut registry)?;
    let control = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(threads),
        cancellation,
    )
    .with_compute_pool(pool);
    registry.execute(
        Algorithm::Cluster(ClusterAlgorithm::Components),
        graph,
        &control,
    )
}

fn component_fingerprint(output: &AlgorithmOutput) -> Vec<([u8; 16], i64)> {
    output
        .rows()
        .iter()
        .map(|row| match (&row[0], &row[1]) {
            (AlgorithmValue::Uuid(uuid), AlgorithmValue::Int64(community)) => (*uuid, *community),
            _ => panic!("expected components uuid/community row"),
        })
        .collect()
}

fn adversarial_components_graph() -> AdjacencyGraph {
    let nodes = 512_u64;
    let group = 128_u64;
    let mut edges = Vec::new();
    for source in 0..nodes {
        let base = (source / group) * group;
        let local = source - base;
        for hop in 1..=40 {
            edges.push((source, base + ((local + hop) % group)));
        }
    }
    AdjacencyGraph::with_test_edges(nodes, &edges)
}

#[test]
fn components_assigns_stable_ids_for_weak_components() {
    let graph = AdjacencyGraph::with_test_edges(6, &[(0, 1), (2, 3), (2, 3), (3, 3)]);
    let output = execute_components(&graph, AlgorithmLimits::default()).unwrap();
    assert_eq!(
        output.rows(),
        [0_i64, 0, 1, 1, 2, 3]
            .into_iter()
            .enumerate()
            .map(|(node, community)| vec![
                AlgorithmValue::Uuid((node as u128).to_be_bytes()),
                AlgorithmValue::Int64(community),
            ])
            .collect::<Vec<_>>()
    );
    assert_eq!(
        output.schema,
        Algorithm::Cluster(ClusterAlgorithm::Components).result_schema()
    );

    let mut registry = AlgorithmRegistry::default();
    register_cluster_algorithms(&mut registry).unwrap();
    assert_eq!(registry.capabilities()[0].dependency, BUILTIN_REVIEW);
}

#[test]
fn components_handles_empty_graphs_and_shared_limits() {
    assert!(
        execute_components(&AdjacencyGraph::default(), AlgorithmLimits::default())
            .unwrap()
            .rows()
            .is_empty()
    );
    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
    assert_eq!(
        execute_components(
            &graph,
            AlgorithmLimits {
                nodes: 2,
                ..AlgorithmLimits::default()
            }
        ),
        Err(AlgorithmError::NodeLimit {
            observed: 3,
            limit: 2,
        })
    );
    assert_eq!(
        execute_components(
            &graph,
            AlgorithmLimits {
                edges: 1,
                ..AlgorithmLimits::default()
            }
        ),
        Err(AlgorithmError::EdgeLimit {
            observed: 2,
            limit: 1,
        })
    );
    assert_eq!(
        execute_components(
            &graph,
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            }
        ),
        Err(AlgorithmError::IterationLimit {
            observed: 1,
            limit: 0,
        })
    );

    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    let mut registry = AlgorithmRegistry::default();
    register_cluster_algorithms(&mut registry).unwrap();
    assert_eq!(
        registry.execute(
            Algorithm::Cluster(ClusterAlgorithm::Components),
            &graph,
            &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
        ),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
fn components_path_selection_respects_crossover_pool_and_one_thread() {
    let no_pool = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    );
    assert_eq!(
        select_components_path(&no_pool, 64, COMPONENTS_PARALLEL_CROSSOVER_EDGES),
        ComponentsExecutionPath::Serial
    );

    let one = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(1),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
    assert_eq!(
        select_components_path(&one, 64, COMPONENTS_PARALLEL_CROSSOVER_EDGES),
        ComponentsExecutionPath::Serial
    );

    let parallel = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
    assert_eq!(
        select_components_path(&parallel, 64, COMPONENTS_PARALLEL_CROSSOVER_EDGES - 1),
        ComponentsExecutionPath::Serial
    );
    assert_eq!(
        select_components_path(&parallel, 64, COMPONENTS_PARALLEL_CROSSOVER_EDGES),
        ComponentsExecutionPath::Parallel {
            threads: 4,
            chunks: 4
        }
    );
}

#[test]
fn component_source_chunks_cover_canonical_ranges() {
    assert_eq!(component_source_chunks(0, 4), Vec::<(usize, usize)>::new());
    assert_eq!(component_source_chunks(5, 1), vec![(0, 5)]);
    assert_eq!(component_source_chunks(5, 2), vec![(0, 3), (3, 5)]);
    assert_eq!(
        component_source_chunks(8, 4),
        vec![(0, 2), (2, 4), (4, 6), (6, 8)]
    );
    assert_eq!(component_source_chunks(3, 8), vec![(0, 1), (1, 2), (2, 3)]);
}

#[test]
fn components_thread_matrix_matches_one_thread_fingerprint() {
    let graph = adversarial_components_graph();
    assert!(graph.edge_entry_count() >= COMPONENTS_PARALLEL_CROSSOVER_EDGES);
    let serial = execute_components_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap();
    let serial_rows = serial.rows();
    let serial_fingerprint = component_fingerprint(&serial);
    assert_eq!(
        community_ids(&serial),
        (0..512)
            .map(|node| i64::from(node / 128))
            .collect::<Vec<_>>()
    );

    for threads in [2_usize, 4, 8] {
        let parallel =
            execute_components_with_pool(&graph, threads, AlgorithmCancellation::default())
                .unwrap();
        assert_eq!(parallel.schema, serial.schema);
        assert_eq!(parallel.rows(), serial_rows);
        assert_eq!(component_fingerprint(&parallel), serial_fingerprint);
    }
}

#[test]
fn components_parallel_cancellation_returns_structured_cancelled() {
    let graph = adversarial_components_graph();
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute_components_with_pool(&graph, 4, cancellation),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
#[ignore = "manual crossover measurement; run with --ignored --nocapture"]
fn measure_components_parallel_crossover() {
    use std::time::Instant;

    for &(nodes, fanout) in &[(128_u64, 16_u64), (256, 32), (512, 40), (1024, 48)] {
        let mut edges = Vec::new();
        for source in 0..nodes {
            for hop in 1..=fanout {
                edges.push((source, (source + hop) % nodes));
            }
        }
        let graph = AdjacencyGraph::with_test_edges(nodes, &edges);
        let serial_control = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: u64::MAX,
                ..AlgorithmLimits::default().with_compute_threads(1)
            },
            AlgorithmCancellation::default(),
        );
        let parallel_control = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: u64::MAX,
                ..AlgorithmLimits::default().with_compute_threads(4)
            },
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));

        let mut serial_ns = u128::MAX;
        let mut parallel_ns = u128::MAX;
        for _ in 0..5 {
            let start = Instant::now();
            let mut parents = (0..graph.node_ids().len()).collect::<Vec<_>>();
            let indices = graph
                .node_ids()
                .iter()
                .enumerate()
                .map(|(index, &node_id)| (node_id, index))
                .collect::<HashMap<_, _>>();
            components_union_serial(&graph, &indices, &mut parents, &serial_control).unwrap();
            serial_ns = serial_ns.min(start.elapsed().as_nanos());

            let start = Instant::now();
            let mut parents = (0..graph.node_ids().len()).collect::<Vec<_>>();
            components_union_parallel(&graph, &indices, &mut parents, &parallel_control).unwrap();
            parallel_ns = parallel_ns.min(start.elapsed().as_nanos());
        }
        println!(
            "components nodes={nodes} fanout={fanout} edges={} serial={}ns parallel={}ns",
            graph.edge_entry_count(),
            serial_ns,
            parallel_ns
        );
    }
}
