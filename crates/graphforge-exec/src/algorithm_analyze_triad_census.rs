use std::collections::{BTreeMap, BTreeSet};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const CHECKPOINT_INTERVAL: usize = 4_096;

pub(crate) const TRIAD_NAMES: [&str; 16] = [
    "003", "012", "102", "021D", "021U", "021C", "111D", "111U", "030T", "030C", "201", "120D",
    "120U", "120C", "210", "300",
];

// Indexed by the six ordered-pair presence bits described in `triad_code`.
const TRIAD_INDEX: [usize; 64] = [
    0, 1, 1, 2, 1, 3, 5, 7, 1, 5, 4, 6, 2, 7, 6, 10, 1, 5, 3, 7, 4, 8, 8, 12, 5, 9, 8, 13, 6, 13,
    11, 14, 1, 4, 5, 6, 5, 8, 9, 13, 3, 8, 8, 11, 7, 12, 13, 14, 2, 6, 7, 10, 6, 11, 13, 14, 7, 13,
    12, 14, 10, 14, 14, 15,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TriadEdge {
    pub edge: [u8; 16],
    pub source: [u8; 16],
    pub target: [u8; 16],
}

/// Count the sixteen MAN triad classes in a directed simple projection.
pub(crate) fn triad_census(
    nodes: &[[u8; 16]],
    edges: &[TriadEdge],
    control: &AlgorithmControl,
) -> Result<[u64; 16], AlgorithmError> {
    control.checkpoint()?;
    control.check_output_rows(16)?;
    let mut work = 0;
    let index = index_nodes(nodes, control, &mut work)?;
    let successors = directed_neighbors(edges, &index, control, &mut work)?;
    let neighbors = weak_neighbors(&successors);
    let mut counts = [0_u64; 16];

    // Batagelj-Mrvar: visit only triads incident to a dyad, then derive 003.
    for v in 0..neighbors.len() {
        checkpoint(control, &mut work)?;
        for &u in neighbors[v].range(v.saturating_add(1)..) {
            checkpoint(control, &mut work)?;
            let union = neighbors[v]
                .union(&neighbors[u])
                .copied()
                .filter(|&w| w != v && w != u)
                .collect::<BTreeSet<_>>();
            for &w in &union {
                checkpoint(control, &mut work)?;
                if u < w || (v < w && w < u && !neighbors[w].contains(&v)) {
                    let class = TRIAD_INDEX[triad_code(v, u, w, &successors)];
                    counts[class] = increment(counts[class])?;
                }
            }
            let dyadic = u64::try_from(neighbors.len() - union.len() - 2)
                .map_err(|_| execution("triad_census exceeds supported range"))?;
            let class = if successors[v].contains(&u) && successors[u].contains(&v) {
                2
            } else {
                1
            };
            counts[class] = add(counts[class], dyadic)?;
        }
    }

    let total = choose_three(neighbors.len())?;
    let connected = counts[1..]
        .iter()
        .try_fold(0_u64, |sum, &value| add(sum, value))?;
    counts[0] = total
        .checked_sub(connected)
        .ok_or_else(|| execution("triad_census invariant exceeds V choose 3"))?;
    if counts
        .iter()
        .try_fold(0_u64, |sum, &value| add(sum, value))?
        != total
    {
        return Err(execution(
            "triad_census invariant does not equal V choose 3",
        ));
    }
    Ok(counts)
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
            return Err(execution("triad_census node UUIDs must be unique"));
        }
    }
    Ok(index)
}

fn directed_neighbors(
    edges: &[TriadEdge],
    index: &BTreeMap<[u8; 16], usize>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<BTreeSet<usize>>, AlgorithmError> {
    let mut successors = vec![BTreeSet::new(); index.len()];
    let mut stored = BTreeMap::new();
    for &edge in edges {
        checkpoint(control, work)?;
        let Some(&source) = index.get(&edge.source) else {
            return Err(execution(
                "triad_census edge endpoint is outside node selection",
            ));
        };
        let Some(&target) = index.get(&edge.target) else {
            return Err(execution(
                "triad_census edge endpoint is outside node selection",
            ));
        };
        if let Some(previous) = stored.insert(edge.edge, edge) {
            if previous != edge {
                return Err(execution(
                    "triad_census edge UUID has inconsistent adjacency entries",
                ));
            }
            continue;
        }
        if source != target {
            successors[source].insert(target);
        }
    }
    Ok(successors)
}

fn weak_neighbors(successors: &[BTreeSet<usize>]) -> Vec<BTreeSet<usize>> {
    let mut neighbors = successors.to_vec();
    for (source, targets) in successors.iter().enumerate() {
        for &target in targets {
            neighbors[target].insert(source);
        }
    }
    neighbors
}

fn triad_code(a: usize, b: usize, c: usize, successors: &[BTreeSet<usize>]) -> usize {
    [(a, b), (b, a), (a, c), (c, a), (b, c), (c, b)]
        .into_iter()
        .enumerate()
        .fold(0, |code, (bit, (source, target))| {
            code | (usize::from(successors[source].contains(&target)) << bit)
        })
}

fn choose_three(n: usize) -> Result<u64, AlgorithmError> {
    let n = u64::try_from(n).map_err(|_| execution("triad_census exceeds supported range"))?;
    if n < 3 {
        return Ok(0);
    }
    let mut factors = [n, n - 1, n - 2];
    for divisor in [2_u64, 3] {
        let factor = factors
            .iter_mut()
            .find(|factor| **factor % divisor == 0)
            .expect("three consecutive integers contain required divisor");
        *factor /= divisor;
    }
    factors.into_iter().try_fold(1_u64, |product, factor| {
        product
            .checked_mul(factor)
            .ok_or_else(|| execution("triad_census exceeds supported range"))
    })
}

fn increment(value: u64) -> Result<u64, AlgorithmError> {
    add(value, 1)
}

fn add(left: u64, right: u64) -> Result<u64, AlgorithmError> {
    left.checked_add(right)
        .ok_or_else(|| execution("triad_census exceeds supported range"))
}

fn checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    *work = work.saturating_add(1);
    if work.is_multiple_of(CHECKPOINT_INTERVAL) {
        control.checkpoint().map(|_| ())
    } else {
        control.check_cancelled()
    }
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

    fn edge(id: u8, source: u8, target: u8) -> TriadEdge {
        TriadEdge {
            edge: uuid(id),
            source: uuid(source),
            target: uuid(target),
        }
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    #[test]
    fn classifies_every_ordered_pair_pattern_in_standard_order() {
        for code in 0_u8..64 {
            let edges = [(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)]
                .into_iter()
                .enumerate()
                .filter(|(bit, _)| code & (1 << bit) != 0)
                .map(|(bit, (source, target))| edge(10 + bit as u8, source, target))
                .collect::<Vec<_>>();
            let counts = triad_census(&[uuid(0), uuid(1), uuid(2)], &edges, &control()).unwrap();
            assert_eq!(counts[TRIAD_INDEX[usize::from(code)]], 1, "code {code}");
            assert_eq!(counts.iter().sum::<u64>(), 1);
        }
        assert_eq!(TRIAD_NAMES[0], "003");
        assert_eq!(TRIAD_NAMES[15], "300");
    }

    #[test]
    fn handles_trivial_edgeless_and_normalized_directed_graphs() {
        assert_eq!(triad_census(&[], &[], &control()).unwrap(), [0; 16]);
        assert_eq!(
            triad_census(&[uuid(0), uuid(1)], &[], &control()).unwrap(),
            [0; 16]
        );
        let edgeless =
            triad_census(&[uuid(0), uuid(1), uuid(2), uuid(3)], &[], &control()).unwrap();
        assert_eq!(edgeless[0], 4);
        let normalized = triad_census(
            &[uuid(0), uuid(1), uuid(2)],
            &[
                edge(10, 0, 1),
                edge(10, 0, 1),
                edge(11, 0, 1),
                edge(12, 1, 0),
                edge(13, 0, 0),
            ],
            &control(),
        )
        .unwrap();
        assert_eq!(normalized[2], 1);
    }

    #[test]
    fn rejects_bad_identity_and_obeys_shared_controls() {
        assert!(triad_census(&[uuid(0), uuid(0)], &[], &control()).is_err());
        assert!(triad_census(&[uuid(0)], &[edge(1, 0, 2)], &control()).is_err());
        assert!(
            triad_census(
                &[uuid(0), uuid(1), uuid(2)],
                &[edge(1, 0, 1), edge(1, 0, 2)],
                &control()
            )
            .is_err()
        );
        let no_output = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 15,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            triad_census(&[], &[], &no_output),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            triad_census(
                &[],
                &[],
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
            ),
            Err(AlgorithmError::Cancelled)
        );
    }
}
