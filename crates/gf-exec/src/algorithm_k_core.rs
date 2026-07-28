//! Exact deterministic core numbers shared by Rust rank and cluster handlers.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_graph::AdjacencyGraph;
use crate::algorithm_neighbors::simple_undirected_neighbors;

pub(crate) fn k_core_numbers(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    let neighbors = simple_undirected_neighbors(graph, control)?;
    let mut degrees: Vec<usize> = neighbors.iter().map(Vec::len).collect();
    let mut queue = BinaryHeap::with_capacity(degrees.len());
    for (node, &degree) in degrees.iter().enumerate() {
        queue.push(Reverse((degree, node)));
    }
    let mut removed = vec![false; degrees.len()];
    let mut cores = vec![0_usize; degrees.len()];
    let mut visited_neighbors = 0_usize;
    let mut processed_entries = 0_usize;
    while let Some(Reverse((degree, node))) = queue.pop() {
        if processed_entries.is_multiple_of(1024) {
            control.checkpoint()?;
        }
        processed_entries += 1;
        if removed[node] || degrees[node] != degree {
            continue;
        }
        removed[node] = true;
        cores[node] = degree;
        for &neighbor in &neighbors[node] {
            if visited_neighbors.is_multiple_of(1024) {
                control.checkpoint()?;
            }
            visited_neighbors += 1;
            if !removed[neighbor] && degrees[neighbor] > degree {
                degrees[neighbor] = degrees[neighbor]
                    .checked_sub(1)
                    .ok_or_else(|| execution("k-core degree underflow"))?;
                queue.push(Reverse((degrees[neighbor], neighbor)));
            }
        }
    }
    Ok(cores)
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

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    #[test]
    fn peels_exact_disconnected_core_numbers() {
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
        assert_eq!(
            k_core_numbers(&graph, &control()).unwrap(),
            [3, 3, 3, 3, 1, 1, 0, 2, 2, 2]
        );
    }

    #[test]
    fn ignores_multiplicity_reciprocals_and_self_loops() {
        let graph =
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (0, 1), (1, 0), (1, 2), (2, 0), (0, 0)]);
        assert_eq!(k_core_numbers(&graph, &control()).unwrap(), [2, 2, 2, 0]);
        assert!(
            k_core_numbers(&AdjacencyGraph::default(), &control())
                .unwrap()
                .is_empty()
        );
    }
}
