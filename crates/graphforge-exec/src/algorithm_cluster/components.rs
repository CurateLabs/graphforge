//! Components execution.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, AlgorithmValue, AssertUnwindSafe, BUILTIN_REVIEW,
    COMPONENTS_PARALLEL_CROSSOVER_EDGES, ClusterAlgorithm, HashMap, IntoParallelRefIterator,
    ParallelIterator, RustAlgorithm, catch_unwind, checkpoint_chunk, execution,
};

pub(super) struct Components;

const COMPONENTS_CHECKPOINT_EDGES: usize = 16_384;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ComponentsExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

#[derive(Debug)]
struct ComponentsChunk {
    links: Vec<(usize, usize)>,
}

impl RustAlgorithm for Components {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::Components),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let node_count = graph.node_ids().len();
        let mut parents: Vec<usize> = (0..node_count).collect();
        let indices: HashMap<u64, usize> = graph
            .node_ids()
            .iter()
            .enumerate()
            .map(|(index, &node_id)| (node_id, index))
            .collect();
        match select_components_path(control, node_count, graph.edge_entry_count()) {
            ComponentsExecutionPath::Serial => {
                components_union_serial(graph, &indices, &mut parents, control)?;
            }
            ComponentsExecutionPath::Parallel { .. } => {
                components_union_parallel(graph, &indices, &mut parents, control)?;
            }
        }

        let mut ids = HashMap::new();
        let algorithm = Algorithm::Cluster(ClusterAlgorithm::Components);
        let mut sink = control.output_sink(algorithm)?;
        for (index, &node_id) in graph.node_ids().iter().enumerate() {
            if index.is_multiple_of(COMPONENTS_CHECKPOINT_EDGES) {
                control.checkpoint()?;
            }
            let root = find(&mut parents, index);
            let community_id = if let Some(&id) = ids.get(&root) {
                id
            } else {
                let id = i64::try_from(ids.len()).map_err(|_| AlgorithmError::Execution {
                    message: "component count exceeds Int64 result range".into(),
                })?;
                ids.insert(root, id);
                id
            };
            let uuid = graph
                .node_uuid(node_id)
                .ok_or_else(|| AlgorithmError::Execution {
                    message: "selected node has no UUID identity".into(),
                })?;
            sink.append_row(&[
                AlgorithmValue::Uuid(uuid),
                AlgorithmValue::Int64(community_id),
            ])?;
        }
        sink.finish()
    }
}

fn components_union_serial(
    graph: &AdjacencyGraph,
    indices: &HashMap<u64, usize>,
    parents: &mut [usize],
    control: &AlgorithmControl,
) -> Result<(), AlgorithmError> {
    let mut visited_edges = 0_usize;
    for (source_index, &source_id) in graph.node_ids().iter().enumerate() {
        for edge in graph.neighbors(source_id) {
            if visited_edges.is_multiple_of(COMPONENTS_CHECKPOINT_EDGES) {
                control.checkpoint()?;
            }
            visited_edges += 1;
            let target_index = indices
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            union(parents, source_index, target_index);
        }
    }
    Ok(())
}

fn components_union_parallel(
    graph: &AdjacencyGraph,
    indices: &HashMap<u64, usize>,
    parents: &mut [usize],
    control: &AlgorithmControl,
) -> Result<(), AlgorithmError> {
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel components requires an instance-owned compute pool"))?;
    control.checkpoint()?;
    let ranges = component_source_chunks(graph.node_ids().len(), control.compute_threads());
    let chunk_results = run_components_on_pool(pool, || {
        Ok(ranges
            .par_iter()
            .map(|&(start, end)| components_chunk_links(graph, indices, start, end, control))
            .collect::<Vec<_>>())
    })?;

    let mut work = 0_usize;
    for chunk in chunk_results {
        for (root, index) in chunk?.links {
            checkpoint_chunk(control, &mut work)?;
            union(parents, root, index);
        }
    }
    Ok(())
}

fn components_chunk_links(
    graph: &AdjacencyGraph,
    indices: &HashMap<u64, usize>,
    start: usize,
    end: usize,
    control: &AlgorithmControl,
) -> Result<ComponentsChunk, AlgorithmError> {
    control.check_cancelled()?;
    let mut parents = HashMap::new();
    let mut visited_edges = 0_usize;
    for (offset, &source_id) in graph.node_ids()[start..end].iter().enumerate() {
        let source_index = start + offset;
        for edge in graph.neighbors(source_id) {
            if visited_edges.is_multiple_of(COMPONENTS_CHECKPOINT_EDGES) {
                control.check_cancelled()?;
            }
            visited_edges += 1;
            let target_index = indices
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            local_union(&mut parents, source_index, target_index);
        }
    }
    control.check_cancelled()?;

    let mut touched = parents.keys().copied().collect::<Vec<_>>();
    touched.sort_unstable();
    let mut links = Vec::with_capacity(touched.len());
    for index in touched {
        let root = local_find(&mut parents, index);
        if index != root {
            links.push((root, index));
        }
    }
    Ok(ComponentsChunk { links })
}

/// Choose serial vs private-pool parallel execution for connected components.
fn select_components_path(
    control: &AlgorithmControl,
    nodes: usize,
    edge_entries: u64,
) -> ComponentsExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || nodes <= 1
        || edge_entries < COMPONENTS_PARALLEL_CROSSOVER_EDGES
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return ComponentsExecutionPath::Serial;
    }
    let chunks = component_source_chunks(nodes, threads).len();
    if chunks <= 1 {
        ComponentsExecutionPath::Serial
    } else {
        ComponentsExecutionPath::Parallel { threads, chunks }
    }
}

fn component_source_chunks(nodes: usize, threads: usize) -> Vec<(usize, usize)> {
    if nodes == 0 {
        return Vec::new();
    }
    let workers = threads.clamp(1, nodes);
    let base = nodes / workers;
    let rem = nodes % workers;
    let mut ranges = Vec::with_capacity(workers);
    let mut start = 0;
    for index in 0..workers {
        let len = base + usize::from(index < rem);
        let end = start + len;
        if start < end {
            ranges.push((start, end));
        }
        start = end;
    }
    ranges
}

fn local_find(parents: &mut HashMap<usize, usize>, index: usize) -> usize {
    parents.entry(index).or_insert(index);
    let mut root = index;
    loop {
        let parent = *parents.entry(root).or_insert(root);
        if parent == root {
            break;
        }
        root = parent;
    }

    let mut cursor = index;
    while cursor != root {
        let parent = parents.insert(cursor, root).unwrap_or(root);
        cursor = parent;
    }
    root
}

fn local_union(parents: &mut HashMap<usize, usize>, left: usize, right: usize) {
    let left = local_find(parents, left);
    let right = local_find(parents, right);
    let (first, second) = if left <= right {
        (left, right)
    } else {
        (right, left)
    };
    parents.insert(second, first);
}

fn run_components_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("components worker panicked")),
    }
}

fn find(parents: &mut [usize], index: usize) -> usize {
    if parents[index] != index {
        parents[index] = find(parents, parents[index]);
    }
    parents[index]
}

fn union(parents: &mut [usize], left: usize, right: usize) {
    let left = find(parents, left);
    let right = find(parents, right);
    let (first, second) = if left <= right {
        (left, right)
    } else {
        (right, left)
    };
    parents[second] = first;
}

#[cfg(test)]
mod tests;
