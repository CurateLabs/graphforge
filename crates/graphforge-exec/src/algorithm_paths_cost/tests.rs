use arrow::array::{Array, FixedSizeBinaryArray, Float64Array};
use arrow::record_batch::RecordBatch;

use super::*;
use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits, AlgorithmRegistry};
use crate::algorithm_paths::register_path_algorithms;

fn uuid(node: u64) -> [u8; 16] {
    u128::from(node).to_be_bytes()
}

/// Deterministic xorshift, so a failing case reproduces.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

struct Case {
    nodes: u64,
    edges: Vec<(u64, u64)>,
    weights: Vec<f64>,
}

impl Case {
    fn graph(&self) -> AdjacencyGraph {
        AdjacencyGraph::with_test_edges(self.nodes, &self.edges)
            .with_test_edge_weights(&self.weights)
    }
}

/// A random multigraph with self-loops and parallel edges. Weights mix zero,
/// repeated values, ties and values whose sums round (`0.1`, `0.7`, `1e-9`).
fn random_case(seed: u64) -> Case {
    let mut random = Random(0x9E37_79B9_7F4A_7C15 ^ seed.wrapping_mul(0xD1B5_4A32_D192_ED03));
    let nodes = 1 + random.below(40);
    let density = 1 + random.below(4);
    let count = random.below(nodes * density + 1);
    let palette = [
        0.0,
        0.0,
        0.1,
        0.1,
        0.7,
        1.0,
        1.0,
        2.5,
        3.3,
        1e-9,
        1e9,
        0.1 + 0.2,
    ];
    let mut edges = Vec::new();
    let mut weights = Vec::new();
    for _ in 0..count {
        edges.push((random.below(nodes), random.below(nodes)));
        weights.push(palette[random.below(palette.len() as u64) as usize]);
    }
    Case {
        nodes,
        edges,
        weights,
    }
}

fn run(
    algorithm: PathAlgorithm,
    graph: &AdjacencyGraph,
    source: u64,
    target: Option<u64>,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    register_path_algorithms(&mut registry, uuid(source), target.map(uuid), 1, None, None)?;
    let control =
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
    registry.execute(Algorithm::Paths(algorithm), graph, &control)
}

/// `(target, cost bits)` of every row, checking the source column.
fn rows(batch: &RecordBatch, source: u64) -> Vec<(u64, u64)> {
    let sources = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let targets = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let costs = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| {
            assert_eq!(sources.value(row), uuid(source));
            let target = u128::from_be_bytes(targets.value(row).try_into().unwrap());
            (target as u64, costs.value(row).to_bits())
        })
        .collect()
}

/// Level-set BFS, written independently of the kernel: expand one whole level
/// at a time and list each level's nodes ascending.
fn level_sets(case: &Case, source: u64) -> Vec<(u64, u64)> {
    let mut seen = vec![false; case.nodes as usize];
    seen[source as usize] = true;
    let mut level = vec![source];
    let mut depth = 0_u32;
    let mut out = Vec::new();
    while !level.is_empty() {
        level.sort_unstable();
        out.extend(level.iter().map(|&n| (n, f64::from(depth).to_bits())));
        let mut next = Vec::new();
        for &node in &level {
            for &(from, to) in &case.edges {
                if from == node && !seen[to as usize] {
                    seen[to as usize] = true;
                    next.push(to);
                }
            }
        }
        level = next;
        depth += 1;
    }
    out
}

/// Bellman-Ford to a fixed point: relax every edge until nothing improves.
/// Non-negative float addition is monotonic, so the fixed point is the minimum
/// over all walks of their left-to-right sums, the value Dijkstra computes.
fn bellman_ford(case: &Case, source: u64) -> Vec<(u64, u64)> {
    let mut dist = vec![f64::INFINITY; case.nodes as usize];
    dist[source as usize] = 0.0;
    loop {
        let mut changed = false;
        for (&(from, to), &weight) in case.edges.iter().zip(&case.weights) {
            let candidate = dist[from as usize] + weight;
            if candidate < dist[to as usize] {
                dist[to as usize] = candidate;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    dist.iter()
        .enumerate()
        .filter(|(_, cost)| cost.is_finite())
        .map(|(node, cost)| (node as u64, cost.to_bits()))
        .collect()
}

fn legacy_projection(output: &AlgorithmOutput, source: u64) -> Vec<(u64, u64)> {
    rows(output.record_batch(), source)
}

#[test]
fn bfs_depth_matches_bfs_and_a_level_set_reference_on_random_graphs() {
    let mut compared = 0;
    for seed in 0..300 {
        let case = random_case(seed);
        let graph = case.graph();
        let source = seed % case.nodes;
        let depth = run(PathAlgorithm::BfsDepth, &graph, source, None).unwrap();
        let bfs = run(PathAlgorithm::Bfs, &graph, source, None).unwrap();
        let got = rows(depth.record_batch(), source);
        assert_eq!(got, legacy_projection(&bfs, source), "seed {seed}: vs bfs");
        assert_eq!(got, level_sets(&case, source), "seed {seed}: vs level sets");
        // Every row appears once, in ascending (depth, node) order.
        assert!(got.windows(2).all(|pair| pair[0] != pair[1]));

        for target in 0..case.nodes {
            let single = run(PathAlgorithm::BfsDepth, &graph, source, Some(target)).unwrap();
            let expected: Vec<_> = got.iter().copied().filter(|r| r.0 == target).collect();
            assert_eq!(
                rows(single.record_batch(), source),
                expected,
                "seed {seed} target {target}"
            );
            compared += 1;
        }
    }
    assert!(compared > 3_000);
}

#[test]
fn dijkstra_cost_matches_dijkstra_and_bellman_ford_bit_for_bit_on_random_graphs() {
    let mut reachable_rows = 0;
    for seed in 0..300 {
        let case = random_case(seed);
        let graph = case.graph();
        let source = seed % case.nodes;
        let cost = run(PathAlgorithm::DijkstraCost, &graph, source, None).unwrap();
        let dijkstra = run(PathAlgorithm::Dijkstra, &graph, source, None).unwrap();
        let got = rows(cost.record_batch(), source);
        assert_eq!(
            got,
            legacy_projection(&dijkstra, source),
            "seed {seed}: vs dijkstra"
        );
        assert_eq!(
            got,
            bellman_ford(&case, source),
            "seed {seed}: vs bellman-ford"
        );
        reachable_rows += got.len();

        for target in 0..case.nodes {
            let single = run(PathAlgorithm::DijkstraCost, &graph, source, Some(target)).unwrap();
            let expected: Vec<_> = got.iter().copied().filter(|r| r.0 == target).collect();
            assert_eq!(
                rows(single.record_batch(), source),
                expected,
                "seed {seed} target {target}"
            );
            let legacy = run(PathAlgorithm::Dijkstra, &graph, source, Some(target)).unwrap();
            assert_eq!(
                rows(single.record_batch(), source),
                legacy_projection(&legacy, source),
                "seed {seed} target {target}: vs dijkstra"
            );
        }
    }
    assert!(reachable_rows > 600, "{reachable_rows} reachable rows");
}

#[test]
fn unweighted_dijkstra_cost_is_the_hop_count() {
    let graph = AdjacencyGraph::with_test_edges(5, &[(0, 1), (1, 2), (0, 2), (2, 3)]);
    let cost = run(PathAlgorithm::DijkstraCost, &graph, 0, None).unwrap();
    let depth = run(PathAlgorithm::BfsDepth, &graph, 0, None).unwrap();
    assert_eq!(rows(cost.record_batch(), 0), rows(depth.record_batch(), 0));
    // Node 4 has no inbound edge: no row at all, not a row with a sentinel.
    assert_eq!(rows(cost.record_batch(), 0).len(), 4);
}

#[test]
fn source_row_has_cost_zero_and_an_unreachable_target_has_no_row() {
    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1)]);
    for algorithm in [PathAlgorithm::BfsDepth, PathAlgorithm::DijkstraCost] {
        let own = run(algorithm, &graph, 0, Some(0)).unwrap();
        assert_eq!(rows(own.record_batch(), 0), vec![(0, 0.0_f64.to_bits())]);
        let missing = run(algorithm, &graph, 0, Some(2)).unwrap();
        assert_eq!(missing.num_rows(), 0);
        let isolated = run(algorithm, &graph, 2, None).unwrap();
        assert_eq!(
            rows(isolated.record_batch(), 2),
            vec![(2, 0.0_f64.to_bits())]
        );
    }
}

#[test]
fn bfs_depth_orders_each_level_by_node_not_by_discovery() {
    // Discovery order of level 1 is 3, 1, 2; the result lists 1, 2, 3.
    let graph = AdjacencyGraph::with_test_edges(5, &[(0, 3), (0, 1), (0, 2), (3, 4), (1, 4)]);
    let output = run(PathAlgorithm::BfsDepth, &graph, 0, None).unwrap();
    let order: Vec<u64> = rows(output.record_batch(), 0).iter().map(|r| r.0).collect();
    assert_eq!(order, vec![0, 1, 2, 3, 4]);
}

#[test]
fn dijkstra_cost_refuses_negative_and_non_finite_weights_even_when_unreachable() {
    for bad in [-1.0, f64::NAN, f64::INFINITY] {
        let graph = AdjacencyGraph::with_test_edges(4, &[(0, 1), (2, 3)])
            .with_test_edge_weights(&[1.0, bad]);
        let error = run(PathAlgorithm::DijkstraCost, &graph, 0, None).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("requires finite non-negative edge weights"),
            "{bad}: {error}"
        );
    }
}

#[test]
fn dijkstra_cost_refuses_an_overflowing_cost() {
    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)])
        .with_test_edge_weights(&[f64::MAX, f64::MAX]);
    let error = run(PathAlgorithm::DijkstraCost, &graph, 0, None).unwrap_err();
    assert!(error.to_string().contains("not finite"), "{error}");
}

#[test]
fn endpoints_must_be_selected_nodes() {
    let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
    for algorithm in [PathAlgorithm::BfsDepth, PathAlgorithm::DijkstraCost] {
        let source = run(algorithm, &graph, 9, None).unwrap_err();
        assert!(source.to_string().contains("source UUID"), "{source}");
        let target = run(algorithm, &graph, 0, Some(9)).unwrap_err();
        assert!(target.to_string().contains("target UUID"), "{target}");
    }
}

#[test]
fn result_is_one_exactly_sized_batch_with_the_canonical_schema() {
    let case = random_case(7);
    let graph = case.graph();
    for algorithm in [PathAlgorithm::BfsDepth, PathAlgorithm::DijkstraCost] {
        let output = run(algorithm, &graph, 0, None).unwrap();
        assert_eq!(output.internal_batch_count, 1);
        let batch = output.record_batch();
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            ["source_uuid", "target_uuid", "cost"]
        );
        assert_eq!(
            batch.schema().metadata().get("graphforge.algorithm"),
            Some(&algorithm.as_str().to_owned())
        );
        // Each column was built with the row count as its capacity, so no
        // buffer holds a doubled allocation.
        for column in batch.columns() {
            let data = column.to_data();
            let used = data.len() * data.data_type().primitive_width().unwrap_or(0);
            let held: usize = data.buffers().iter().map(|buffer| buffer.capacity()).sum();
            assert!(
                held <= used.next_multiple_of(64) + 64,
                "{algorithm}: a column holds {held} bytes for {used} used"
            );
        }
    }
}

#[test]
fn output_limit_refuses_before_building_the_columns() {
    let graph =
        AdjacencyGraph::with_test_edges(50, &(0..49).map(|n| (n, n + 1)).collect::<Vec<_>>());
    // Straight to the handler, so only its own check can refuse.
    let handlers: [Box<dyn RustAlgorithm>; 2] = [
        Box::new(BfsDepth {
            source: uuid(0),
            target: None,
        }),
        Box::new(DijkstraCost {
            source: uuid(0),
            target: None,
        }),
    ];
    for handler in handlers {
        let control = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 10,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        let error = handler.execute(&graph, &control).unwrap_err();
        assert!(
            matches!(
                error,
                AlgorithmError::OutputLimit {
                    observed: 50,
                    limit: 10
                }
            ),
            "{error:?}"
        );
    }
}

#[test]
fn a_cancelled_search_stops() {
    let edges: Vec<(u64, u64)> = (0..20_000).map(|n| (n, n + 1)).collect();
    let graph = AdjacencyGraph::with_test_edges(20_001, &edges);
    // Straight to the handler: the registry's own check would hide a search
    // that never polls.
    let handlers: [Box<dyn RustAlgorithm>; 2] = [
        Box::new(BfsDepth {
            source: uuid(0),
            target: None,
        }),
        Box::new(DijkstraCost {
            source: uuid(0),
            target: None,
        }),
    ];
    for handler in handlers {
        let cancellation = AlgorithmCancellation::default();
        let control = AlgorithmControl::new(AlgorithmLimits::default(), cancellation.clone());
        cancellation.cancel();
        assert_eq!(
            handler.execute(&graph, &control).unwrap_err(),
            AlgorithmError::Cancelled
        );
    }
}

/// Guards the references above against a silent empty comparison.
#[test]
fn the_references_are_not_vacuous() {
    let case = Case {
        nodes: 4,
        edges: vec![(0, 1), (1, 2), (0, 2)],
        weights: vec![0.1, 0.2, 0.7],
    };
    assert_eq!(
        bellman_ford(&case, 0),
        vec![
            (0, 0.0_f64.to_bits()),
            (1, 0.1_f64.to_bits()),
            (2, (0.1_f64 + 0.2).to_bits())
        ]
    );
    assert_eq!(
        level_sets(&case, 0),
        vec![
            (0, 0.0_f64.to_bits()),
            (1, 1.0_f64.to_bits()),
            (2, 1.0_f64.to_bits())
        ]
    );
}
