//! Cost-only breadth-first and Dijkstra searches over node ordinals (#1974).
//!
//! `bfs_depth` and `dijkstra_cost` answer "how far is every reachable node from
//! the source" without materializing a path per node. They keep one cost per
//! node, a visit order or a heap, and emit one exactly-sized Arrow batch of
//! `(source_uuid, target_uuid, cost)`. Row order and every cost bit match the
//! `bfs` and `dijkstra` verbs: ascending `(depth, node)` for BFS and ascending
//! node for Dijkstra. A node that is not reachable emits no row.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, Float64Builder};
use graphforge_core::algorithms::{Algorithm, PathAlgorithm};

use crate::algorithm_arrow_sink::output_from_columns;
use crate::algorithm_dispatch::{
    AlgorithmCapability, AlgorithmControl, AlgorithmError, AlgorithmOutput, DependencyReview,
    RustAlgorithm,
};
use crate::algorithm_graph::AdjacencyGraph;

const BUILTIN_REVIEW: DependencyReview = DependencyReview {
    implementation: "graphforge-exec built-in",
    license: "Apache-2.0",
    maintenance: "GraphForge workspace",
    security: "workspace cargo-deny and CodeQL",
    binary_size: "no additional dependency",
    determinism: "hop count then node order; cost then node order",
    platforms: "Rust workspace targets",
};

/// Cancellation is polled once per this many edges or rows.
const POLL_INTERVAL: usize = 4_096;

/// Depth of a node the search has not reached.
const UNREACHED: u32 = u32::MAX;

pub(crate) struct BfsDepth {
    pub(crate) source: [u8; 16],
    pub(crate) target: Option<[u8; 16]>,
}

pub(crate) struct DijkstraCost {
    pub(crate) source: [u8; 16],
    pub(crate) target: Option<[u8; 16]>,
}

fn execution(message: impl Into<String>) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.into(),
    }
}

/// Counts work and polls cancellation every [`POLL_INTERVAL`] units. A
/// single-pass search is bounded by the node and edge limits, so it never
/// consumes the iteration budget (#1922).
struct Poll<'a> {
    control: &'a AlgorithmControl,
    work: usize,
}

impl<'a> Poll<'a> {
    fn new(control: &'a AlgorithmControl) -> Self {
        Self { control, work: 0 }
    }

    fn tick(&mut self) -> Result<(), AlgorithmError> {
        if self.work.is_multiple_of(POLL_INTERVAL) {
            self.control.check_cancelled()?;
        }
        self.work = self.work.wrapping_add(1);
        Ok(())
    }
}

/// Resolve the source and optional target UUIDs to ordinals.
fn endpoints(
    graph: &AdjacencyGraph,
    name: &str,
    source: &[u8; 16],
    target: Option<&[u8; 16]>,
) -> Result<(usize, Option<usize>), AlgorithmError> {
    let source = graph
        .ordinal_of_uuid(source)
        .ok_or_else(|| execution(format!("{name} source UUID is not in the selected graph")))?;
    let target = target
        .map(|uuid| {
            graph.ordinal_of_uuid(uuid).ok_or_else(|| {
                execution(format!("{name} target UUID is not in the selected graph"))
            })
        })
        .transpose()?;
    Ok((source, target))
}

fn ordinal(value: usize) -> Result<u32, AlgorithmError> {
    u32::try_from(value).map_err(|_| execution("node ordinal exceeds supported range"))
}

/// Emit `(source, node, cost)` for each of `rows` nodes as one batch built with
/// exact column capacity.
fn cost_output(
    algorithm: PathAlgorithm,
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
    source: &[u8; 16],
    rows: usize,
    nodes: impl Iterator<Item = (usize, f64)>,
) -> Result<AlgorithmOutput, AlgorithmError> {
    control.check_output_rows(rows)?;
    let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    let mut costs = Float64Builder::with_capacity(rows);
    let mut poll = Poll::new(control);
    for (node, cost) in nodes {
        poll.tick()?;
        let uuid = graph
            .uuid_of_ordinal(node)
            .ok_or_else(|| execution(format!("{algorithm} target has no UUID identity")))?;
        sources
            .append_value(source)
            .and(targets.append_value(uuid))
            .map_err(|error| execution(error.to_string()))?;
        costs.append_value(cost);
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(sources.finish()),
        Arc::new(targets.finish()),
        Arc::new(costs.finish()),
    ];
    output_from_columns(Algorithm::Paths(algorithm), columns)
}

impl RustAlgorithm for BfsDepth {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Paths(PathAlgorithm::BfsDepth),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let (source, target) = endpoints(graph, "bfs_depth", &self.source, self.target.as_ref())?;
        let node_count = graph.node_count();
        let mut depth = vec![UNREACHED; node_count];
        // Nodes in visit order. A level is a contiguous run, and sorting each
        // run ascending makes the whole list the `(depth, node)` order of `bfs`.
        // Reserved up front: a push that doubles the buffer would hold the old
        // and the new one at once.
        let mut order: Vec<u32> = Vec::with_capacity(node_count);
        order.push(ordinal(source)?);
        depth[source] = 0;
        let mut poll = Poll::new(control);
        let mut level_start = 0;
        let mut next_depth = 0_u32;
        while level_start < order.len() && target.is_none_or(|node| depth[node] == UNREACHED) {
            next_depth = next_depth
                .checked_add(1)
                .filter(|&value| value != UNREACHED)
                .ok_or_else(|| execution("bfs_depth depth exceeds supported range"))?;
            let level_end = order.len();
            for position in level_start..level_end {
                let node = order[position] as usize;
                for &neighbor in graph.neighbor_ordinals(node) {
                    poll.tick()?;
                    let slot = depth
                        .get_mut(neighbor as usize)
                        .ok_or_else(|| execution("adjacency references an unselected node"))?;
                    if *slot == UNREACHED {
                        *slot = next_depth;
                        order.push(neighbor);
                    }
                }
            }
            order[level_end..].sort_unstable();
            level_start = level_end;
        }

        let cost = |node: usize| f64::from(depth[node]);
        match target {
            Some(node) if depth[node] != UNREACHED => cost_output(
                PathAlgorithm::BfsDepth,
                graph,
                control,
                &self.source,
                1,
                std::iter::once((node, cost(node))),
            ),
            Some(_) => cost_output(
                PathAlgorithm::BfsDepth,
                graph,
                control,
                &self.source,
                0,
                std::iter::empty(),
            ),
            None => cost_output(
                PathAlgorithm::BfsDepth,
                graph,
                control,
                &self.source,
                order.len(),
                order
                    .iter()
                    .map(|&node| (node as usize, cost(node as usize))),
            ),
        }
    }
}

/// A tentative cost, ordered so that `BinaryHeap` pops the smallest cost and,
/// among equal costs, the smallest node.
#[derive(Clone, Copy, Debug)]
struct Frontier {
    cost: f64,
    node: u32,
}

impl PartialEq for Frontier {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Frontier {}

impl Ord for Frontier {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .cost
            .total_cmp(&self.cost)
            .then_with(|| other.node.cmp(&self.node))
    }
}

impl PartialOrd for Frontier {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// One bit per node.
struct Bitset(Vec<u64>);

impl Bitset {
    fn new(len: usize) -> Self {
        Self(vec![0; len.div_ceil(64)])
    }

    fn contains(&self, index: usize) -> bool {
        self.0[index / 64] >> (index % 64) & 1 == 1
    }

    fn insert(&mut self, index: usize) {
        self.0[index / 64] |= 1 << (index % 64);
    }
}

impl RustAlgorithm for DijkstraCost {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Paths(PathAlgorithm::DijkstraCost),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let (source, target) =
            endpoints(graph, "dijkstra_cost", &self.source, self.target.as_ref())?;
        let node_count = graph.node_count();
        let mut poll = Poll::new(control);
        // Like `dijkstra`, refuse any negative or non-finite weight in the
        // selected graph, reachable or not.
        for node in 0..node_count {
            for &weight in graph.neighbor_weights(node).unwrap_or_default() {
                poll.tick()?;
                if !weight.is_finite() || weight < 0.0 {
                    return Err(execution(
                        "dijkstra_cost requires finite non-negative edge weights",
                    ));
                }
            }
        }

        let mut dist = vec![f64::INFINITY; node_count];
        let mut settled = Bitset::new(node_count);
        let mut heap = BinaryHeap::from([Frontier {
            cost: 0.0,
            node: ordinal(source)?,
        }]);
        dist[source] = 0.0;
        while let Some(Frontier { cost, node }) = heap.pop() {
            let node = node as usize;
            // Lazy deletion: a settled node, or an entry a cheaper one replaced.
            if settled.contains(node) || cost.total_cmp(&dist[node]) != Ordering::Equal {
                continue;
            }
            settled.insert(node);
            if target == Some(node) {
                break;
            }
            let neighbors = graph.neighbor_ordinals(node);
            let weights = graph.neighbor_weights(node);
            for (slot, &neighbor) in neighbors.iter().enumerate() {
                poll.tick()?;
                let neighbor = neighbor as usize;
                if neighbor >= node_count {
                    return Err(execution("adjacency references an unselected node"));
                }
                if settled.contains(neighbor) {
                    continue;
                }
                let candidate = cost + weights.map_or(1.0, |weights| weights[slot]);
                if !candidate.is_finite() {
                    return Err(execution("dijkstra_cost accumulated cost is not finite"));
                }
                if candidate < dist[neighbor] {
                    dist[neighbor] = candidate;
                    heap.push(Frontier {
                        cost: candidate,
                        node: ordinal(neighbor)?,
                    });
                }
            }
        }
        drop(heap);

        // A node is reached exactly when it was settled; tentative costs of
        // unsettled nodes (after a target stop) are not results.
        match target {
            Some(node) if settled.contains(node) => cost_output(
                PathAlgorithm::DijkstraCost,
                graph,
                control,
                &self.source,
                1,
                std::iter::once((node, dist[node])),
            ),
            Some(_) => cost_output(
                PathAlgorithm::DijkstraCost,
                graph,
                control,
                &self.source,
                0,
                std::iter::empty(),
            ),
            None => {
                let reached = (0..node_count).filter(|&node| settled.contains(node));
                cost_output(
                    PathAlgorithm::DijkstraCost,
                    graph,
                    control,
                    &self.source,
                    reached.clone().count(),
                    reached.map(|node| (node, dist[node])),
                )
            }
        }
    }
}

#[cfg(test)]
mod tests;
