//! Exact deterministic automorphism counting over `automorphism-ir-v1`.
//!
//! `automorphism-count-ir-v1` pairs two individualization/refinement trees for
//! the same normalized graph. UUID order schedules structurally equal
//! candidates, but only adjacency multiplicity decides whether a leaf is an
//! automorphism.

use crate::algorithm_analyze_automorphism::{AutomorphismGraph, AutomorphismPartition};
use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const MAX_SEARCH_DEPTH: usize = 1_024;
const MAX_SEARCH_STATE_ENTRIES: u64 = 16_000_000;

#[derive(Debug)]
struct SearchBudget {
    used: u64,
    limit: u64,
}

impl SearchBudget {
    fn new(limit: u64) -> Self {
        Self { used: 0, limit }
    }

    fn consume(
        &mut self,
        entries: usize,
        control: &AlgorithmControl,
    ) -> Result<(), AlgorithmError> {
        control.checkpoint()?;
        let entries =
            u64::try_from(entries).map_err(|_| AlgorithmError::AutomorphismCountStateLimit {
                observed: u64::MAX,
                limit: self.limit,
            })?;
        let observed =
            self.used
                .checked_add(entries)
                .ok_or(AlgorithmError::AutomorphismCountStateLimit {
                    observed: u64::MAX,
                    limit: self.limit,
                })?;
        if observed > self.limit {
            return Err(AlgorithmError::AutomorphismCountStateLimit {
                observed,
                limit: self.limit,
            });
        }
        self.used = observed;
        Ok(())
    }
}

/// Count every adjacency-multiplicity-preserving permutation exactly once.
pub(crate) fn count_automorphisms(
    graph: &AutomorphismGraph,
    control: &AlgorithmControl,
) -> Result<u64, AlgorithmError> {
    count_with_state_limit(graph, control, MAX_SEARCH_STATE_ENTRIES)
}

fn count_with_state_limit(
    graph: &AutomorphismGraph,
    control: &AlgorithmControl,
    state_limit: u64,
) -> Result<u64, AlgorithmError> {
    let mut budget = SearchBudget::new(state_limit);
    preflight(graph.node_count(), &mut budget, control)?;
    let partition = graph.equitable_partition(control)?;
    search(graph, &partition, &partition, 0, &mut budget, control)
}

fn search(
    graph: &AutomorphismGraph,
    domain: &AutomorphismPartition,
    image: &AutomorphismPartition,
    depth: usize,
    budget: &mut SearchBudget,
    control: &AlgorithmControl,
) -> Result<u64, AlgorithmError> {
    control.checkpoint()?;
    if depth > MAX_SEARCH_DEPTH {
        return Err(execution(format!(
            "automorphism count search depth limit exceeded: observed {depth}, limit {MAX_SEARCH_DEPTH}"
        )));
    }
    if !same_shape(domain, image) {
        return Ok(0);
    }

    if fully_symmetric_cells(graph, domain, image, budget, control)? {
        return cell_factorial_product(domain, control);
    }

    let Some(cell_index) = domain.cells().iter().position(|cell| cell.len() > 1) else {
        return verify_leaf(graph, domain, image, budget, control);
    };
    let pivot = domain.cells()[cell_index][0];
    let candidates = &image.cells()[cell_index];
    reserve_individualization(domain, budget, control)?;
    let individualized_domain = graph.individualize(domain, pivot, control)?;
    let mut count = 0_u64;
    for &candidate in candidates {
        control.checkpoint()?;
        reserve_individualization(image, budget, control)?;
        let individualized_image = graph.individualize(image, candidate, control)?;
        let branch = search(
            graph,
            &individualized_domain,
            &individualized_image,
            depth + 1,
            budget,
            control,
        )?;
        count = count
            .checked_add(branch)
            .ok_or(AlgorithmError::AutomorphismCountOverflow)?;
    }
    Ok(count)
}

/// If every transposition generated within every paired cell preserves the
/// graph, the direct product of those symmetric groups is exactly the remaining
/// automorphism set for this state.
fn fully_symmetric_cells(
    graph: &AutomorphismGraph,
    domain: &AutomorphismPartition,
    image: &AutomorphismPartition,
    budget: &mut SearchBudget,
    control: &AlgorithmControl,
) -> Result<bool, AlgorithmError> {
    let Some(base) = paired_permutation(domain, image, budget, control)? else {
        return Ok(false);
    };
    reserve_verification(base.len(), budget, control)?;
    if !graph.preserves_adjacency(&base, control)? {
        return Ok(false);
    }
    for (domain_cell, image_cell) in domain.cells().iter().zip(image.cells()) {
        let Some((&anchor, remaining)) = domain_cell.split_first() else {
            continue;
        };
        for (&other, &other_image) in remaining.iter().zip(&image_cell[1..]) {
            control.checkpoint()?;
            let mut permutation = clone_indices(&base, "symmetry permutation", budget, control)?;
            permutation[anchor] = other_image;
            permutation[other] = image_cell[0];
            reserve_verification(permutation.len(), budget, control)?;
            if !graph.preserves_adjacency(&permutation, control)? {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn verify_leaf(
    graph: &AutomorphismGraph,
    domain: &AutomorphismPartition,
    image: &AutomorphismPartition,
    budget: &mut SearchBudget,
    control: &AlgorithmControl,
) -> Result<u64, AlgorithmError> {
    let Some(permutation) = paired_permutation(domain, image, budget, control)? else {
        return Ok(0);
    };
    reserve_verification(permutation.len(), budget, control)?;
    Ok(u64::from(graph.preserves_adjacency(&permutation, control)?))
}

fn paired_permutation(
    domain: &AutomorphismPartition,
    image: &AutomorphismPartition,
    budget: &mut SearchBudget,
    control: &AlgorithmControl,
) -> Result<Option<Vec<usize>>, AlgorithmError> {
    control.checkpoint()?;
    if !same_shape(domain, image) {
        return Ok(None);
    }
    let node_count = domain.cells().iter().map(Vec::len).sum();
    let mut permutation = reserved_indices(node_count, "leaf permutation", budget, control)?;
    permutation.resize(node_count, usize::MAX);
    for (domain_cell, image_cell) in domain.cells().iter().zip(image.cells()) {
        for (&source, &target) in domain_cell.iter().zip(image_cell) {
            let Some(slot) = permutation.get_mut(source) else {
                return Err(execution(
                    "automorphism count partition member is out of range",
                ));
            };
            if *slot != usize::MAX || target >= node_count {
                return Err(execution(
                    "automorphism count partition is not a node permutation",
                ));
            }
            *slot = target;
        }
    }
    if permutation.contains(&usize::MAX) {
        return Err(execution(
            "automorphism count partition does not cover every node",
        ));
    }
    Ok(Some(permutation))
}

fn same_shape(left: &AutomorphismPartition, right: &AutomorphismPartition) -> bool {
    left.cell_sizes().eq(right.cell_sizes())
}

fn cell_factorial_product(
    partition: &AutomorphismPartition,
    control: &AlgorithmControl,
) -> Result<u64, AlgorithmError> {
    let mut product = 1_u64;
    for cell in partition.cells() {
        control.checkpoint()?;
        let mut factorial = 1_u64;
        for factor in 2..=cell.len() {
            control.check_cancelled()?;
            factorial = factorial
                .checked_mul(
                    u64::try_from(factor).map_err(|_| AlgorithmError::AutomorphismCountOverflow)?,
                )
                .ok_or(AlgorithmError::AutomorphismCountOverflow)?;
        }
        product = product
            .checked_mul(factorial)
            .ok_or(AlgorithmError::AutomorphismCountOverflow)?;
    }
    Ok(product)
}

fn preflight(
    node_count: usize,
    budget: &mut SearchBudget,
    control: &AlgorithmControl,
) -> Result<(), AlgorithmError> {
    budget.consume(
        equitable_partition_capacity_entries(node_count, budget.limit)?,
        control,
    )
}

fn reserve_individualization(
    partition: &AutomorphismPartition,
    budget: &mut SearchBudget,
    control: &AlgorithmControl,
) -> Result<(), AlgorithmError> {
    let node_count = partition
        .cells()
        .iter()
        .map(Vec::len)
        .try_fold(0_usize, usize::checked_add)
        .ok_or(AlgorithmError::AutomorphismCountStateLimit {
            observed: u64::MAX,
            limit: budget.limit,
        })?;
    let entries = individualization_capacity_entries(node_count, budget.limit)?;
    budget.consume(entries, control)
}

fn equitable_partition_capacity_entries(
    node_count: usize,
    limit: u64,
) -> Result<usize, AlgorithmError> {
    // Each initial signature owns three scalar slots, followed by canonical
    // order and color vectors.
    let initial = node_count
        .checked_mul(5)
        .ok_or(AlgorithmError::AutomorphismCountStateLimit {
            observed: u64::MAX,
            limit,
        })?;
    initial
        .checked_add(refinement_capacity_entries(node_count, limit)?)
        .ok_or(AlgorithmError::AutomorphismCountStateLimit {
            observed: u64::MAX,
            limit,
        })
}

fn individualization_capacity_entries(
    node_count: usize,
    limit: u64,
) -> Result<usize, AlgorithmError> {
    // Individualization clones the current colors before full refinement.
    node_count
        .checked_add(refinement_capacity_entries(node_count, limit)?)
        .ok_or(AlgorithmError::AutomorphismCountStateLimit {
            observed: u64::MAX,
            limit,
        })
}

fn refinement_capacity_entries(node_count: usize, limit: u64) -> Result<usize, AlgorithmError> {
    // Every non-stable 1-WL round strictly increases the color count, so there
    // are at most max(n, 1) rounds. Per round reserve directed outgoing and
    // incoming payloads (2*n^2), each signature's scalar plus two Vec headers
    // (7*n pointer-sized slots), canonical order/colors (2*n), and a
    // conservative final partition's cell headers/members (4*n).
    let quadratic = node_count
        .checked_mul(node_count)
        .and_then(|value| value.checked_mul(2))
        .ok_or(AlgorithmError::AutomorphismCountStateLimit {
            observed: u64::MAX,
            limit,
        })?;
    let linear = node_count
        .checked_mul(13)
        .ok_or(AlgorithmError::AutomorphismCountStateLimit {
            observed: u64::MAX,
            limit,
        })?;
    quadratic
        .checked_add(linear)
        .and_then(|per_round| per_round.checked_mul(node_count.max(1)))
        .ok_or(AlgorithmError::AutomorphismCountStateLimit {
            observed: u64::MAX,
            limit,
        })
}

fn reserve_verification(
    node_count: usize,
    budget: &mut SearchBudget,
    control: &AlgorithmControl,
) -> Result<(), AlgorithmError> {
    // `preserves_adjacency` allocates one membership bit per node.
    budget.consume(node_count, control)
}

fn reserved_indices(
    length: usize,
    name: &str,
    budget: &mut SearchBudget,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    control.checkpoint()?;
    budget.consume(length, control)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(length)
        .map_err(|_| execution(format!("automorphism count {name} allocation failed")))?;
    Ok(values)
}

fn clone_indices(
    values: &[usize],
    name: &str,
    budget: &mut SearchBudget,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    control.checkpoint()?;
    let mut cloned = reserved_indices(values.len(), name, budget, control)?;
    cloned.extend_from_slice(values);
    Ok(cloned)
}

fn execution(message: impl Into<String>) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_analyze_automorphism::AutomorphismEdge;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits};

    fn uuid(value: u8) -> [u8; 16] {
        [value; 16]
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn graph(node_count: u8, edges: &[(u8, u8)], directed: bool) -> AutomorphismGraph {
        let nodes = (0..node_count).map(uuid).collect::<Vec<_>>();
        let edges = edges
            .iter()
            .enumerate()
            .map(|(index, &(source, target))| AutomorphismEdge {
                edge: uuid(u8::try_from(index + 100).unwrap()),
                source: uuid(source),
                target: uuid(target),
            })
            .collect::<Vec<_>>();
        AutomorphismGraph::try_new(&nodes, &edges, directed, &control()).unwrap()
    }

    fn count(graph: &AutomorphismGraph) -> u64 {
        count_automorphisms(graph, &control()).unwrap()
    }

    #[test]
    fn exact_factorial_and_basic_families() {
        assert_eq!(count(&graph(0, &[], false)), 1);
        assert_eq!(count(&graph(1, &[], false)), 1);
        assert_eq!(count(&graph(5, &[], false)), 120);
        assert_eq!(
            count(&graph(
                5,
                &[
                    (0, 1),
                    (0, 2),
                    (0, 3),
                    (0, 4),
                    (1, 2),
                    (1, 3),
                    (1, 4),
                    (2, 3),
                    (2, 4),
                    (3, 4),
                ],
                false,
            )),
            120
        );
        assert_eq!(
            count(&graph(5, &[(0, 1), (1, 2), (2, 3), (3, 4), (4, 0)], false)),
            10
        );
        assert_eq!(count(&graph(4, &[(0, 1), (1, 2), (2, 3)], false)), 2);
    }

    #[test]
    fn directed_loops_parallel_reciprocal_and_disconnected_are_exact() {
        assert_eq!(
            count(&graph(
                4,
                &[(0, 0), (1, 1), (0, 1), (0, 1), (1, 0), (2, 3), (3, 2)],
                true,
            )),
            2
        );
        assert_eq!(
            count(&graph(
                6,
                &[(0, 1), (1, 2), (2, 0), (3, 4), (4, 5), (5, 3)],
                false
            )),
            72
        );
    }

    #[test]
    fn asymmetric_and_uuid_renaming_are_deterministic() {
        let asymmetric = graph(
            6,
            &[(0, 1), (0, 2), (1, 2), (1, 3), (2, 4), (3, 4), (4, 5)],
            true,
        );
        assert_eq!(count(&asymmetric), 1);
        assert_eq!(count(&asymmetric), count(&asymmetric));

        let nodes = [uuid(90), uuid(2), uuid(70), uuid(4), uuid(50)];
        let edges = [
            AutomorphismEdge {
                edge: uuid(101),
                source: nodes[0],
                target: nodes[1],
            },
            AutomorphismEdge {
                edge: uuid(102),
                source: nodes[1],
                target: nodes[2],
            },
            AutomorphismEdge {
                edge: uuid(103),
                source: nodes[2],
                target: nodes[3],
            },
            AutomorphismEdge {
                edge: uuid(104),
                source: nodes[3],
                target: nodes[4],
            },
            AutomorphismEdge {
                edge: uuid(105),
                source: nodes[4],
                target: nodes[0],
            },
        ];
        let renamed = AutomorphismGraph::try_new(&nodes, &edges, false, &control()).unwrap();
        assert_eq!(count(&renamed), 10);
    }

    #[test]
    fn factorial_overflow_is_structured_without_partial_result() {
        assert_eq!(
            count_automorphisms(&graph(21, &[], false), &control()),
            Err(AlgorithmError::AutomorphismCountOverflow)
        );
    }

    #[test]
    fn cancellation_and_iteration_exhaustion_are_structured() {
        let graph = graph(4, &[(0, 1), (1, 2), (2, 3), (3, 0)], false);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            count_automorphisms(
                &graph,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
            ),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            count_automorphisms(
                &graph,
                &AlgorithmControl::new(
                    AlgorithmLimits {
                        iterations: 0,
                        ..AlgorithmLimits::default()
                    },
                    AlgorithmCancellation::default(),
                )
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
    }

    #[test]
    fn search_state_preflight_and_incremental_exhaustion_are_structured() {
        let singleton = graph(1, &[], false);
        let preflight_entries = equitable_partition_capacity_entries(1, u64::MAX).unwrap();
        let preflight_only_control = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            count_with_state_limit(
                &singleton,
                &preflight_only_control,
                u64::try_from(preflight_entries - 1).unwrap(),
            ),
            Err(AlgorithmError::AutomorphismCountStateLimit {
                observed: u64::try_from(preflight_entries).unwrap(),
                limit: u64::try_from(preflight_entries - 1).unwrap(),
            })
        );

        let path = graph(4, &[(0, 1), (1, 2), (2, 3)], false);
        let preflight_entries = equitable_partition_capacity_entries(4, u64::MAX).unwrap();
        assert!(matches!(
            count_with_state_limit(
                &path,
                &control(),
                u64::try_from(preflight_entries).unwrap(),
            ),
            Err(AlgorithmError::AutomorphismCountStateLimit {
                observed,
                limit,
            }) if observed > limit
        ));
    }

    #[test]
    fn exact_exhaustion_precedes_individualization_and_verification_allocations() {
        let path = graph(4, &[(0, 1), (1, 2), (2, 3)], false);
        let partition = path.equitable_partition(&control()).unwrap();
        let individualization_entries = individualization_capacity_entries(4, u64::MAX).unwrap();
        let mut budget = SearchBudget::new(u64::try_from(individualization_entries - 1).unwrap());
        let one_checkpoint = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            reserve_individualization(&partition, &mut budget, &one_checkpoint),
            Err(AlgorithmError::AutomorphismCountStateLimit {
                observed: u64::try_from(individualization_entries).unwrap(),
                limit: u64::try_from(individualization_entries - 1).unwrap(),
            })
        );

        let singleton = graph(1, &[], false);
        let singleton_partition = singleton.equitable_partition(&control()).unwrap();
        let mut budget = SearchBudget::new(1);
        assert_eq!(
            verify_leaf(
                &singleton,
                &singleton_partition,
                &singleton_partition,
                &mut budget,
                &control(),
            ),
            Err(AlgorithmError::AutomorphismCountStateLimit {
                observed: 2,
                limit: 1,
            })
        );
    }

    #[test]
    fn depth_cancellation_and_malformed_pair_boundaries_are_atomic() {
        let candidate_graph = graph(4, &[(0, 1), (1, 2), (2, 3)], false);
        let partition = candidate_graph.equitable_partition(&control()).unwrap();
        let mut budget = SearchBudget::new(MAX_SEARCH_STATE_ENTRIES);
        assert!(matches!(
            search(
                &candidate_graph,
                &partition,
                &partition,
                MAX_SEARCH_DEPTH + 1,
                &mut budget,
                &control(),
            ),
            Err(AlgorithmError::Execution { message })
                if message.contains("search depth limit exceeded")
        ));

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            cell_factorial_product(
                &partition,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );

        let other = graph(3, &[(0, 1)], false);
        let other_partition = other.equitable_partition(&control()).unwrap();
        let mut budget = SearchBudget::new(MAX_SEARCH_STATE_ENTRIES);
        assert_eq!(
            paired_permutation(&partition, &other_partition, &mut budget, &control()).unwrap(),
            None
        );
    }

    #[test]
    fn exhaustive_small_graphs_match_brute_force() {
        for directed in [false, true] {
            let maximum_nodes = if directed { 3 } else { 4 };
            for node_count in 0_u8..=maximum_nodes {
                let slots = if directed {
                    usize::from(node_count) * usize::from(node_count)
                } else {
                    usize::from(node_count) * usize::from(node_count.saturating_add(1)) / 2
                };
                for mask in 0_u64..(1_u64 << slots) {
                    let edges = mask_edges(node_count, mask, directed);
                    let graph = graph(node_count, &edges, directed);
                    assert_eq!(
                        count(&graph),
                        brute_force_count(usize::from(node_count), &edges, directed),
                        "directed={directed}, nodes={node_count}, mask={mask}"
                    );
                }
            }
        }
    }

    fn mask_edges(node_count: u8, mask: u64, directed: bool) -> Vec<(u8, u8)> {
        let mut edges = Vec::new();
        let mut bit = 0;
        for source in 0..node_count {
            for target in 0..node_count {
                if !directed && target < source {
                    continue;
                }
                if mask & (1 << bit) != 0 {
                    edges.push((source, target));
                }
                bit += 1;
            }
        }
        edges
    }

    fn brute_force_count(node_count: usize, edges: &[(u8, u8)], directed: bool) -> u64 {
        let mut adjacency = vec![0_u64; node_count * node_count];
        for &(source, target) in edges {
            let source = usize::from(source);
            let target = usize::from(target);
            adjacency[source * node_count + target] += 1;
            if !directed && source != target {
                adjacency[target * node_count + source] += 1;
            }
        }
        let mut permutation = (0..node_count).collect::<Vec<_>>();
        let mut count = 0;
        permutations(&mut permutation, 0, &mut |candidate| {
            let preserves = (0..node_count).all(|source| {
                (0..node_count).all(|target| {
                    adjacency[source * node_count + target]
                        == adjacency[candidate[source] * node_count + candidate[target]]
                })
            });
            if preserves {
                count += 1;
            }
        });
        count
    }

    fn permutations(values: &mut [usize], start: usize, visit: &mut impl FnMut(&[usize])) {
        if start == values.len() {
            visit(values);
            return;
        }
        for index in start..values.len() {
            values.swap(start, index);
            permutations(values, start + 1, visit);
            values.swap(start, index);
        }
    }
}
