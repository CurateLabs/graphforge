//! Canonical multigraph normalization and refinement for automorphism counting.
//!
//! `automorphism-ir-v1` uses UUIDs only to establish deterministic storage and
//! candidate order. Structural colors and equivalence checks depend exclusively
//! on adjacency multiplicity.

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const CHECKPOINT_INTERVAL: usize = 4_096;
const MAX_MATRIX_ENTRIES: usize = 16_000_000;

/// One adjacency record from the selected graph projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AutomorphismEdge {
    pub edge: [u8; 16],
    pub source: [u8; 16],
    pub target: [u8; 16],
}

/// UUID-indexed, checked adjacency multiplicities for one graph projection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AutomorphismGraph {
    nodes: Vec<[u8; 16]>,
    directed: bool,
    adjacency: Vec<u64>,
}

/// A stable equitable partition whose members follow canonical UUID order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AutomorphismPartition {
    colors: Vec<usize>,
    cells: Vec<Vec<usize>>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct InitialSignature {
    loops: u64,
    outgoing_degree: u64,
    incoming_degree: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RefinementSignature {
    color: usize,
    outgoing: Vec<u64>,
    incoming: Vec<u64>,
}

impl AutomorphismGraph {
    /// Number of normalized nodes, available without allocating search state.
    pub(crate) fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Normalize selected nodes and stored-edge records for `automorphism-ir-v1`.
    pub(crate) fn try_new(
        nodes: &[[u8; 16]],
        edges: &[AutomorphismEdge],
        directed: bool,
        control: &AlgorithmControl,
    ) -> Result<Self, AlgorithmError> {
        control.check_graph_size(nodes.len(), u64::try_from(edges.len()).unwrap_or(u64::MAX))?;
        control.checkpoint()?;
        let mut work = 0_usize;

        let mut ordered_nodes = Vec::new();
        ordered_nodes
            .try_reserve_exact(nodes.len())
            .map_err(|_| allocation("node index"))?;
        ordered_nodes.extend_from_slice(nodes);
        ordered_nodes.sort_unstable();
        if ordered_nodes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(execution("automorphism node UUIDs must be unique"));
        }

        let matrix_len = ordered_nodes
            .len()
            .checked_mul(ordered_nodes.len())
            .ok_or_else(|| overflow("adjacency matrix size"))?;
        if matrix_len > MAX_MATRIX_ENTRIES {
            return Err(execution(format!(
                "automorphism adjacency matrix allocation limit exceeded: observed {matrix_len}, limit {MAX_MATRIX_ENTRIES}"
            )));
        }
        let mut adjacency = Vec::new();
        adjacency
            .try_reserve_exact(matrix_len)
            .map_err(|_| allocation("adjacency matrix"))?;
        adjacency.resize(matrix_len, 0_u64);

        let mut stored = Vec::new();
        stored
            .try_reserve_exact(edges.len())
            .map_err(|_| allocation("stored edge index"))?;
        for &raw in edges {
            checkpoint(control, &mut work)?;
            let source = ordered_nodes
                .binary_search(&raw.source)
                .map_err(|_| execution("automorphism edge endpoint is outside node selection"))?;
            let target = ordered_nodes
                .binary_search(&raw.target)
                .map_err(|_| execution("automorphism edge endpoint is outside node selection"))?;
            let endpoints = if directed || source <= target {
                (source, target)
            } else {
                (target, source)
            };
            stored.push((raw.edge, endpoints));
        }
        stored.sort_unstable_by_key(|record| record.0);

        let node_count = ordered_nodes.len();
        let mut cursor = 0;
        while cursor < stored.len() {
            checkpoint(control, &mut work)?;
            let (edge_uuid, (source, target)) = stored[cursor];
            cursor += 1;
            while cursor < stored.len() && stored[cursor].0 == edge_uuid {
                checkpoint(control, &mut work)?;
                if stored[cursor].1 != (source, target) {
                    return Err(execution(
                        "automorphism edge UUID has inconsistent adjacency entries",
                    ));
                }
                cursor += 1;
            }
            increment(&mut adjacency[index(node_count, source, target)?])?;
            if !directed && source != target {
                increment(&mut adjacency[index(node_count, target, source)?])?;
            }
        }
        control.check_cancelled()?;
        Ok(Self {
            nodes: ordered_nodes,
            directed,
            adjacency,
        })
    }

    /// Compute the stable structure-only equitable partition.
    pub(crate) fn equitable_partition(
        &self,
        control: &AlgorithmControl,
    ) -> Result<AutomorphismPartition, AlgorithmError> {
        control.checkpoint()?;
        let mut work = 0_usize;
        let mut initial = Vec::new();
        initial
            .try_reserve_exact(self.nodes.len())
            .map_err(|_| allocation("initial signatures"))?;
        for node in 0..self.nodes.len() {
            checkpoint(control, &mut work)?;
            initial.push(self.initial_signature(node, control, &mut work)?);
        }
        let colors = canonical_colors(&initial, control, &mut work)?;
        self.refine(colors, control, &mut work)
    }

    /// Individualize one node, then restore equitable refinement.
    pub(crate) fn individualize(
        &self,
        partition: &AutomorphismPartition,
        node: usize,
        control: &AlgorithmControl,
    ) -> Result<AutomorphismPartition, AlgorithmError> {
        if partition.colors.len() != self.nodes.len() || node >= self.nodes.len() {
            return Err(execution(
                "automorphism individualization does not match graph",
            ));
        }
        let mut colors = clone_vec(&partition.colors, "individualized colors")?;
        colors[node] = colors
            .iter()
            .copied()
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| overflow("individualized color"))?;
        self.refine(colors, control, &mut 0)
    }

    /// Verify a complete candidate permutation against every multiplicity.
    pub(crate) fn preserves_adjacency(
        &self,
        permutation: &[usize],
        control: &AlgorithmControl,
    ) -> Result<bool, AlgorithmError> {
        if permutation.len() != self.nodes.len() {
            return Ok(false);
        }
        let mut seen = zeroed_bool(self.nodes.len(), "permutation membership")?;
        let mut work = 0_usize;
        for &node in permutation {
            checkpoint(control, &mut work)?;
            if node >= self.nodes.len() || seen[node] {
                return Ok(false);
            }
            seen[node] = true;
        }
        for (source, &mapped_source) in permutation.iter().enumerate() {
            for (target, &mapped_target) in permutation.iter().enumerate() {
                checkpoint(control, &mut work)?;
                if self.multiplicity(source, target)?
                    != self.multiplicity(mapped_source, mapped_target)?
                {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    fn refine(
        &self,
        mut colors: Vec<usize>,
        control: &AlgorithmControl,
        work: &mut usize,
    ) -> Result<AutomorphismPartition, AlgorithmError> {
        loop {
            control.checkpoint()?;
            let color_count = colors.iter().copied().max().map_or(0, |color| color + 1);
            let mut signatures = Vec::new();
            signatures
                .try_reserve_exact(self.nodes.len())
                .map_err(|_| allocation("refinement signatures"))?;
            for (node, &node_color) in colors.iter().enumerate() {
                checkpoint(control, work)?;
                let mut outgoing = zeroed(color_count, "outgoing refinement signature")?;
                let mut incoming = if self.directed {
                    zeroed(color_count, "incoming refinement signature")?
                } else {
                    Vec::new()
                };
                for (neighbor, &color) in colors.iter().enumerate() {
                    checkpoint(control, work)?;
                    add(&mut outgoing[color], self.multiplicity(node, neighbor)?)?;
                    if self.directed {
                        add(&mut incoming[color], self.multiplicity(neighbor, node)?)?;
                    }
                }
                signatures.push(RefinementSignature {
                    color: node_color,
                    outgoing,
                    incoming,
                });
            }
            let refined = canonical_colors(&signatures, control, work)?;
            if refined == colors {
                return AutomorphismPartition::from_colors(colors);
            }
            colors = refined;
        }
    }

    fn initial_signature(
        &self,
        node: usize,
        control: &AlgorithmControl,
        work: &mut usize,
    ) -> Result<InitialSignature, AlgorithmError> {
        let loops = self.multiplicity(node, node)?;
        let mut outgoing_degree = 0_u64;
        let mut incoming_degree = 0_u64;
        for neighbor in 0..self.nodes.len() {
            checkpoint(control, work)?;
            let outgoing = self.multiplicity(node, neighbor)?;
            let incoming = self.multiplicity(neighbor, node)?;
            if !self.directed && neighbor == node {
                add(&mut outgoing_degree, outgoing)?;
                add(&mut outgoing_degree, outgoing)?;
            } else {
                add(&mut outgoing_degree, outgoing)?;
            }
            if self.directed {
                add(&mut incoming_degree, incoming)?;
            }
        }
        if !self.directed {
            incoming_degree = outgoing_degree;
        }
        Ok(InitialSignature {
            loops,
            outgoing_degree,
            incoming_degree,
        })
    }

    fn multiplicity(&self, source: usize, target: usize) -> Result<u64, AlgorithmError> {
        self.adjacency
            .get(index(self.nodes.len(), source, target)?)
            .copied()
            .ok_or_else(|| execution("automorphism adjacency index is outside graph"))
    }
}

impl AutomorphismPartition {
    fn from_colors(colors: Vec<usize>) -> Result<Self, AlgorithmError> {
        let cell_count = colors.iter().copied().max().map_or(0, |color| color + 1);
        let mut cells = Vec::new();
        cells
            .try_reserve_exact(cell_count)
            .map_err(|_| allocation("partition cells"))?;
        cells.resize_with(cell_count, Vec::new);
        for (node, &color) in colors.iter().enumerate() {
            cells[color]
                .try_reserve(1)
                .map_err(|_| allocation("partition cell members"))?;
            cells[color].push(node);
        }
        Ok(Self { colors, cells })
    }

    /// Structural cell-size shape, independent of member UUID values.
    pub(crate) fn cell_sizes(&self) -> impl Iterator<Item = usize> + '_ {
        self.cells.iter().map(Vec::len)
    }

    /// Canonically ordered structural cells and their canonically ordered members.
    pub(crate) fn cells(&self) -> &[Vec<usize>] {
        &self.cells
    }
}

fn canonical_colors<T: Ord>(
    signatures: &[T],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<usize>, AlgorithmError> {
    let mut order = Vec::new();
    order
        .try_reserve_exact(signatures.len())
        .map_err(|_| allocation("canonical signature order"))?;
    for index in 0..signatures.len() {
        checkpoint(control, work)?;
        order.push(index);
    }
    order.sort_unstable_by(|&left, &right| {
        signatures[left]
            .cmp(&signatures[right])
            .then_with(|| left.cmp(&right))
    });
    control.check_cancelled()?;
    let mut colors = zeroed_usize(signatures.len(), "canonical colors")?;
    let mut color = 0_usize;
    for (position, &node) in order.iter().enumerate() {
        checkpoint(control, work)?;
        if position > 0 && signatures[node] != signatures[order[position - 1]] {
            color = color
                .checked_add(1)
                .ok_or_else(|| overflow("canonical color count"))?;
        }
        colors[node] = color;
    }
    Ok(colors)
}

fn zeroed(length: usize, name: &str) -> Result<Vec<u64>, AlgorithmError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(length)
        .map_err(|_| allocation(name))?;
    values.resize(length, 0);
    Ok(values)
}

fn zeroed_bool(length: usize, name: &str) -> Result<Vec<bool>, AlgorithmError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(length)
        .map_err(|_| allocation(name))?;
    values.resize(length, false);
    Ok(values)
}

fn zeroed_usize(length: usize, name: &str) -> Result<Vec<usize>, AlgorithmError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(length)
        .map_err(|_| allocation(name))?;
    values.resize(length, 0);
    Ok(values)
}

fn clone_vec<T: Clone>(values: &[T], name: &str) -> Result<Vec<T>, AlgorithmError> {
    let mut cloned = Vec::new();
    cloned
        .try_reserve_exact(values.len())
        .map_err(|_| allocation(name))?;
    cloned.extend_from_slice(values);
    Ok(cloned)
}

fn index(node_count: usize, source: usize, target: usize) -> Result<usize, AlgorithmError> {
    source
        .checked_mul(node_count)
        .and_then(|offset| offset.checked_add(target))
        .ok_or_else(|| overflow("adjacency index"))
}

fn increment(value: &mut u64) -> Result<(), AlgorithmError> {
    add(value, 1)
}

fn add(value: &mut u64, amount: u64) -> Result<(), AlgorithmError> {
    *value = value
        .checked_add(amount)
        .ok_or_else(|| overflow("adjacency multiplicity"))?;
    Ok(())
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

fn allocation(name: &str) -> AlgorithmError {
    execution(format!("automorphism {name} allocation failed"))
}

fn overflow(name: &str) -> AlgorithmError {
    execution(format!("automorphism {name} exceeds supported range"))
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

    fn edge(id: u8, source: u8, target: u8) -> AutomorphismEdge {
        AutomorphismEdge {
            edge: uuid(id),
            source: uuid(source),
            target: uuid(target),
        }
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn graph(nodes: &[u8], edges: &[AutomorphismEdge], directed: bool) -> AutomorphismGraph {
        AutomorphismGraph::try_new(
            &nodes.iter().copied().map(uuid).collect::<Vec<_>>(),
            edges,
            directed,
            &control(),
        )
        .unwrap()
    }

    fn sizes(partition: &AutomorphismPartition) -> Vec<usize> {
        partition.cell_sizes().collect()
    }

    #[test]
    fn normalizes_directed_loops_parallel_edges_and_reciprocals() {
        let graph = graph(
            &[3, 1, 2],
            &[
                edge(10, 1, 1),
                edge(11, 1, 2),
                edge(12, 1, 2),
                edge(13, 2, 1),
                edge(11, 1, 2),
            ],
            true,
        );
        assert_eq!(graph.nodes, vec![uuid(1), uuid(2), uuid(3)]);
        assert_eq!(graph.multiplicity(0, 0), Ok(1));
        assert_eq!(graph.multiplicity(0, 1), Ok(2));
        assert_eq!(graph.multiplicity(1, 0), Ok(1));
        assert_eq!(graph.multiplicity(1, 1), Ok(0));
    }

    #[test]
    fn undirected_mirrors_dedupe_by_edge_uuid_but_parallel_records_remain() {
        let graph = graph(
            &[1, 2],
            &[
                edge(10, 1, 2),
                edge(10, 2, 1),
                edge(11, 2, 1),
                edge(12, 2, 2),
            ],
            false,
        );
        assert_eq!(graph.multiplicity(0, 1), Ok(2));
        assert_eq!(graph.multiplicity(1, 0), Ok(2));
        assert_eq!(graph.multiplicity(1, 1), Ok(1));
    }

    #[test]
    fn rejects_malformed_identity_and_selection() {
        assert!(matches!(
            AutomorphismGraph::try_new(
                &[uuid(1), uuid(1)],
                &[],
                false,
                &control()
            ),
            Err(AlgorithmError::Execution { message })
                if message.contains("node UUIDs must be unique")
        ));
        assert!(matches!(
            AutomorphismGraph::try_new(
                &[uuid(1), uuid(2)],
                &[edge(10, 1, 2), edge(10, 1, 1)],
                false,
                &control()
            ),
            Err(AlgorithmError::Execution { message })
                if message.contains("inconsistent adjacency")
        ));
        assert!(matches!(
            AutomorphismGraph::try_new(
                &[uuid(1)],
                &[edge(10, 1, 2)],
                true,
                &control()
            ),
            Err(AlgorithmError::Execution { message })
                if message.contains("outside node selection")
        ));
    }

    #[test]
    fn empty_and_singleton_refinement_are_stable() {
        let empty = graph(&[], &[], false);
        assert!(
            empty
                .equitable_partition(&control())
                .unwrap()
                .cell_sizes()
                .next()
                .is_none()
        );
        let singleton = graph(&[7], &[edge(1, 7, 7)], false);
        let partition = singleton.equitable_partition(&control()).unwrap();
        assert_eq!(sizes(&partition), vec![1]);
        assert_eq!(
            sizes(&singleton.individualize(&partition, 0, &control()).unwrap()),
            vec![1]
        );
    }

    #[test]
    fn refinement_separates_structural_roles_and_is_idempotent() {
        let graph = graph(
            &[1, 2, 3, 4],
            &[edge(10, 1, 2), edge(11, 2, 3), edge(12, 3, 4)],
            false,
        );
        let partition = graph.equitable_partition(&control()).unwrap();
        assert_eq!(sizes(&partition), vec![2, 2]);
        let individualized = graph.individualize(&partition, 0, &control()).unwrap();
        assert_eq!(sizes(&individualized), vec![1, 1, 1, 1]);
        assert_eq!(
            graph
                .refine(individualized.colors.clone(), &control(), &mut 0)
                .unwrap(),
            individualized
        );
    }

    #[test]
    fn structural_shape_is_invariant_under_uuid_renaming() {
        let left = graph(
            &[1, 2, 3, 4],
            &[edge(10, 1, 2), edge(11, 2, 3), edge(12, 3, 4)],
            false,
        );
        let right = graph(
            &[10, 20, 30, 40],
            &[edge(50, 30, 10), edge(51, 10, 40), edge(52, 40, 20)],
            false,
        );
        let left_partition = left.equitable_partition(&control()).unwrap();
        let right_partition = right.equitable_partition(&control()).unwrap();
        assert!(left_partition.cell_sizes().eq(right_partition.cell_sizes()));
    }

    #[test]
    fn leaf_verifier_matches_brute_force_structure_preservation() {
        let graph = graph(
            &[1, 2, 3],
            &[
                edge(10, 1, 1),
                edge(11, 2, 2),
                edge(12, 1, 3),
                edge(13, 2, 3),
            ],
            false,
        );
        let permutations = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        let accepted = permutations
            .iter()
            .copied()
            .filter(|permutation| graph.preserves_adjacency(permutation, &control()).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(accepted, vec![[0, 1, 2], [1, 0, 2]]);
        assert!(!graph.preserves_adjacency(&[0, 0, 2], &control()).unwrap());
        assert!(!graph.preserves_adjacency(&[0, 1], &control()).unwrap());
    }

    #[test]
    fn directed_leaf_verifier_preserves_orientation() {
        let graph = graph(
            &[1, 2, 3],
            &[edge(10, 1, 2), edge(11, 2, 3), edge(12, 3, 1)],
            true,
        );
        assert!(graph.preserves_adjacency(&[1, 2, 0], &control()).unwrap());
        assert!(!graph.preserves_adjacency(&[0, 2, 1], &control()).unwrap());
    }

    #[test]
    fn cancellation_and_graph_limits_are_structured() {
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            AutomorphismGraph::try_new(
                &[uuid(1)],
                &[],
                false,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
            ),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            AutomorphismGraph::try_new(
                &[uuid(1), uuid(2)],
                &[],
                false,
                &AlgorithmControl::new(
                    AlgorithmLimits {
                        nodes: 1,
                        ..AlgorithmLimits::default()
                    },
                    AlgorithmCancellation::default()
                )
            ),
            Err(AlgorithmError::NodeLimit { .. })
        ));
        assert!(matches!(
            AutomorphismGraph::try_new(
                &[uuid(1)],
                &[edge(1, 1, 1)],
                false,
                &AlgorithmControl::new(
                    AlgorithmLimits {
                        edges: 0,
                        ..AlgorithmLimits::default()
                    },
                    AlgorithmCancellation::default()
                )
            ),
            Err(AlgorithmError::EdgeLimit { .. })
        ));
    }

    #[test]
    fn equitable_refinement_honors_cancellation_and_zero_iteration_budget() {
        let graph = graph(&[1, 2], &[edge(1, 1, 2)], false);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            graph.equitable_partition(&AlgorithmControl::new(
                AlgorithmLimits::default(),
                cancellation
            )),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            graph.equitable_partition(&AlgorithmControl::new(
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            )),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0
            })
        ));
    }

    #[test]
    fn checked_multiplicity_and_matrix_allocation_limits_fail_atomically() {
        let mut value = u64::MAX;
        assert!(matches!(
            increment(&mut value),
            Err(AlgorithmError::Execution { message })
                if message.contains("multiplicity")
        ));
        let nodes = (0_u16..4_001)
            .map(|value| {
                let mut uuid = [0_u8; 16];
                uuid[14..].copy_from_slice(&value.to_be_bytes());
                uuid
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            AutomorphismGraph::try_new(&nodes, &[], false, &control()),
            Err(AlgorithmError::Execution { message })
                if message.contains("matrix allocation limit")
        ));
    }
}
