use std::collections::{BTreeMap, BTreeSet};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const CHECKPOINT_INTERVAL: usize = 4_096;

/// One stored edge entry in the selected public-identity projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ChromaticEdge {
    pub edge: [u8; 16],
    pub source: [u8; 16],
    pub target: [u8; 16],
}

impl ChromaticEdge {
    fn canonical(mut self) -> Self {
        if self.target < self.source {
            std::mem::swap(&mut self.source, &mut self.target);
        }
        self
    }
}

/// Return the exact chromatic number of the selected undirected simple graph.
pub(crate) fn exact_chromatic_number(
    nodes: &[[u8; 16]],
    edges: &[ChromaticEdge],
    control: &AlgorithmControl,
) -> Result<u64, AlgorithmError> {
    control.checkpoint()?;
    control.check_output_rows(1)?;

    let mut work = 0_usize;
    let node_index = index_nodes(nodes, control, &mut work)?;
    let neighbors = simple_neighbors(edges, &node_index, control, &mut work)?;
    if neighbors.is_empty() {
        return Ok(0);
    }

    let mut colors = vec![None; neighbors.len()];
    let mut best = greedy_upper_bound(&neighbors, &mut colors, control, &mut work)?;
    colors.fill(None);
    search(&neighbors, &mut colors, 0, &mut best, control)?;
    u64::try_from(best).map_err(|_| execution("chromatic_number result exceeds UInt64 range"))
}

fn index_nodes(
    nodes: &[[u8; 16]],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<BTreeMap<[u8; 16], usize>, AlgorithmError> {
    let mut ordered = nodes.to_vec();
    ordered.sort_unstable();
    let mut index = BTreeMap::new();
    for (position, uuid) in ordered.into_iter().enumerate() {
        checkpoint(control, work)?;
        if index.insert(uuid, position).is_some() {
            return Err(execution("chromatic_number node UUIDs must be unique"));
        }
    }
    Ok(index)
}

fn simple_neighbors(
    edges: &[ChromaticEdge],
    node_index: &BTreeMap<[u8; 16], usize>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<BTreeSet<usize>>, AlgorithmError> {
    let mut stored = BTreeMap::new();
    for &raw in edges {
        checkpoint(control, work)?;
        let edge = raw.canonical();
        let Some(&source) = node_index.get(&edge.source) else {
            return Err(execution(
                "chromatic_number edge endpoint is outside node selection",
            ));
        };
        let Some(&target) = node_index.get(&edge.target) else {
            return Err(execution(
                "chromatic_number edge endpoint is outside node selection",
            ));
        };
        if source == target {
            return Err(execution(
                "chromatic_number is undefined for a graph containing a self-loop",
            ));
        }
        if let Some(previous) = stored.insert(edge.edge, edge)
            && previous != edge
        {
            return Err(execution(
                "chromatic_number edge UUID has inconsistent adjacency entries",
            ));
        }
    }

    let mut neighbors = vec![BTreeSet::new(); node_index.len()];
    for edge in stored.into_values() {
        checkpoint(control, work)?;
        let source = node_index[&edge.source];
        let target = node_index[&edge.target];
        neighbors[source].insert(target);
        neighbors[target].insert(source);
    }
    Ok(neighbors)
}

fn greedy_upper_bound(
    neighbors: &[BTreeSet<usize>],
    colors: &mut [Option<usize>],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<usize, AlgorithmError> {
    let mut used_count = 0_usize;
    for _ in 0..neighbors.len() {
        checkpoint(control, work)?;
        let vertex = select_vertex(neighbors, colors, control)?;
        let forbidden = neighbor_colors(vertex, neighbors, colors, control)?;
        let mut color = 0_usize;
        while forbidden.contains(&color) {
            checkpoint(control, work)?;
            color = increment(color)?;
        }
        if color == used_count {
            used_count = increment(used_count)?;
        }
        colors[vertex] = Some(color);
    }
    Ok(used_count)
}

fn search(
    neighbors: &[BTreeSet<usize>],
    colors: &mut [Option<usize>],
    used_count: usize,
    best: &mut usize,
    control: &AlgorithmControl,
) -> Result<(), AlgorithmError> {
    control.checkpoint()?;
    if colors.iter().all(Option::is_some) {
        *best = (*best).min(used_count);
        return Ok(());
    }
    if used_count >= *best {
        return Ok(());
    }

    let vertex = select_vertex(neighbors, colors, control)?;
    let forbidden = neighbor_colors(vertex, neighbors, colors, control)?;
    for color in 0..=used_count {
        control.check_cancelled()?;
        if color >= *best || forbidden.contains(&color) {
            continue;
        }
        let next_used = if color == used_count {
            increment(used_count)?
        } else {
            used_count
        };
        colors[vertex] = Some(color);
        search(neighbors, colors, next_used, best, control)?;
        colors[vertex] = None;
    }
    Ok(())
}

/// Choose maximum saturation, then maximum degree, then smallest UUID/index.
fn select_vertex(
    neighbors: &[BTreeSet<usize>],
    colors: &[Option<usize>],
    control: &AlgorithmControl,
) -> Result<usize, AlgorithmError> {
    let mut selected = None;
    let mut selected_key = (0_usize, 0_usize);
    for (vertex, adjacent) in neighbors.iter().enumerate() {
        control.check_cancelled()?;
        if colors[vertex].is_some() {
            continue;
        }
        let saturation = neighbor_colors(vertex, neighbors, colors, control)?.len();
        let key = (saturation, adjacent.len());
        if selected.is_none() || key > selected_key {
            selected = Some(vertex);
            selected_key = key;
        }
    }
    selected.ok_or_else(|| execution("chromatic_number search lost an uncolored node"))
}

fn neighbor_colors(
    vertex: usize,
    neighbors: &[BTreeSet<usize>],
    colors: &[Option<usize>],
    control: &AlgorithmControl,
) -> Result<BTreeSet<usize>, AlgorithmError> {
    let mut used = BTreeSet::new();
    for &neighbor in &neighbors[vertex] {
        control.check_cancelled()?;
        if let Some(color) = colors[neighbor] {
            used.insert(color);
        }
    }
    Ok(used)
}

fn increment(value: usize) -> Result<usize, AlgorithmError> {
    value
        .checked_add(1)
        .ok_or_else(|| execution("chromatic_number exceeds supported range"))
}

fn checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    *work = work.saturating_add(1);
    if work.is_multiple_of(CHECKPOINT_INTERVAL) {
        control.checkpoint()?;
    } else {
        control.check_cancelled()?;
    }
    Ok(())
}

fn execution(message: impl Into<String>) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits};

    fn uuid(value: u8) -> [u8; 16] {
        [value; 16]
    }

    fn edge(id: u8, source: u8, target: u8) -> ChromaticEdge {
        ChromaticEdge {
            edge: uuid(id),
            source: uuid(source),
            target: uuid(target),
        }
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn chromatic(nodes: &[u8], edges: &[(u8, u8, u8)]) -> u64 {
        let nodes = nodes.iter().copied().map(uuid).collect::<Vec<_>>();
        let edges = edges
            .iter()
            .map(|&(id, source, target)| edge(id, source, target))
            .collect::<Vec<_>>();
        exact_chromatic_number(&nodes, &edges, &control()).unwrap()
    }

    #[test]
    fn handles_empty_edgeless_and_complete_graphs_exactly() {
        assert_eq!(chromatic(&[], &[]), 0);
        assert_eq!(chromatic(&[3, 1, 2], &[]), 1);
        assert_eq!(
            chromatic(
                &[0, 1, 2, 3],
                &[
                    (10, 0, 1),
                    (11, 0, 2),
                    (12, 0, 3),
                    (13, 1, 2),
                    (14, 1, 3),
                    (15, 2, 3),
                ],
            ),
            4
        );
    }

    #[test]
    fn distinguishes_odd_and_even_cycles() {
        assert_eq!(
            chromatic(
                &[0, 1, 2, 3, 4],
                &[(10, 0, 1), (11, 1, 2), (12, 2, 3), (13, 3, 4), (14, 4, 0),],
            ),
            3
        );
        assert_eq!(
            chromatic(
                &[0, 1, 2, 3],
                &[(10, 0, 1), (11, 1, 2), (12, 2, 3), (13, 3, 0)],
            ),
            2
        );
    }

    #[test]
    fn handles_bipartite_disconnected_graphs_and_isolates() {
        assert_eq!(
            chromatic(
                &[0, 1, 2, 3, 4, 5, 9],
                &[
                    (10, 0, 3),
                    (11, 0, 4),
                    (12, 1, 3),
                    (13, 1, 5),
                    (14, 2, 4),
                    (15, 2, 5),
                ],
            ),
            2
        );
    }

    #[test]
    fn normalizes_mirrors_parallel_and_reciprocal_edges() {
        assert_eq!(
            chromatic(
                &[0, 1, 2],
                &[(10, 0, 1), (10, 1, 0), (11, 0, 1), (12, 1, 0), (13, 1, 2),],
            ),
            2
        );
    }

    #[test]
    fn rejects_self_loops_and_invalid_identity_atomically() {
        for result in [
            exact_chromatic_number(&[uuid(0)], &[edge(1, 0, 0)], &control()),
            exact_chromatic_number(&[uuid(0), uuid(0)], &[], &control()),
            exact_chromatic_number(&[uuid(0)], &[edge(1, 0, 2)], &control()),
            exact_chromatic_number(
                &[uuid(0), uuid(1), uuid(2)],
                &[edge(1, 0, 1), edge(1, 0, 2)],
                &control(),
            ),
        ] {
            assert!(matches!(result, Err(AlgorithmError::Execution { .. })));
        }
    }

    #[test]
    fn uses_checked_results_and_shared_controls() {
        assert!(matches!(
            increment(usize::MAX),
            Err(AlgorithmError::Execution { .. })
        ));

        let no_output = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            exact_chromatic_number(&[], &[], &no_output),
            Err(AlgorithmError::OutputLimit { .. })
        ));

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            exact_chromatic_number(
                &[],
                &[],
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );

        let one_iteration = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            exact_chromatic_number(&[uuid(0), uuid(1)], &[], &one_iteration),
            Err(AlgorithmError::IterationLimit { .. })
        ));
    }
}
