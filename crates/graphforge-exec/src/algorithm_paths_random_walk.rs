//! Deterministic random walks over graph-native UUID adjacency.

use std::collections::HashMap;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

/// Changing the generator, seed derivation, draw conversion, or choice ordering
/// requires a new contract version.
pub(crate) const RANDOM_WALK_RNG_CONTRACT: &str = "splitmix64-v1";

/// One direction-expanded sampling choice. Parallel edges remain distinct.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct RandomWalkEdge {
    pub(crate) edge_uuid: [u8; 16],
    pub(crate) neighbor_uuid: [u8; 16],
    pub(crate) weight: f64,
}

/// Graph-native adjacency keyed by public node UUID.
pub(crate) type RandomWalkAdjacency = HashMap<[u8; 16], Vec<RandomWalkEdge>>;

/// Lazy source of graph-native choices for one visited node.
pub(crate) trait RandomWalkAdjacencySource {
    fn choices(&self, node: &[u8; 16]) -> Result<Vec<RandomWalkEdge>, AlgorithmError>;
}

impl RandomWalkAdjacencySource for RandomWalkAdjacency {
    fn choices(&self, node: &[u8; 16]) -> Result<Vec<RandomWalkEdge>, AlgorithmError> {
        Ok(self.get(node).cloned().unwrap_or_default())
    }
}

/// UUID walks in source-selector order, then walk ordinal.
pub(crate) fn random_walks<A: RandomWalkAdjacencySource>(
    adjacency: &A,
    sources: &[[u8; 16]],
    walks_per_source: usize,
    walk_length: usize,
    seed: u64,
    weighted: bool,
    control: &AlgorithmControl,
) -> Result<Vec<Vec<[u8; 16]>>, AlgorithmError> {
    control.check_cancelled()?;
    let output_rows =
        sources
            .len()
            .checked_mul(walks_per_source)
            .ok_or_else(|| AlgorithmError::Execution {
                message: "random-walk output product exceeds usize".into(),
            })?;
    let transitions =
        output_rows
            .checked_mul(walk_length)
            .ok_or_else(|| AlgorithmError::Execution {
                message: "random-walk transition product exceeds usize".into(),
            })?;
    control.check_output_rows(output_rows)?;
    control.check_iterations(transitions)?;

    let mut output = Vec::with_capacity(output_rows);
    for (source_ordinal, source_uuid) in sources.iter().enumerate() {
        for walk_ordinal in 0..walks_per_source {
            control.check_cancelled()?;
            let mut rng = SplitMix64::new(derive_seed(seed, source_ordinal, walk_ordinal));
            let mut node = *source_uuid;
            let mut walk = Vec::new();
            walk.push(*source_uuid);

            for _ in 0..walk_length {
                control.checkpoint()?;
                let mut choices = adjacency.choices(&node)?;
                choices.sort_unstable_by_key(|edge| (edge.neighbor_uuid, edge.edge_uuid));
                let choices = choices.iter().collect::<Vec<_>>();
                let Some(edge) = choose(&choices, weighted, &mut rng)? else {
                    break;
                };
                node = edge.neighbor_uuid;
                walk.push(node);
            }
            output.push(walk);
        }
    }
    Ok(output)
}

fn choose<'a>(
    choices: &'a [&RandomWalkEdge],
    weighted: bool,
    rng: &mut SplitMix64,
) -> Result<Option<&'a RandomWalkEdge>, AlgorithmError> {
    if choices.is_empty() {
        return Ok(None);
    }
    if !weighted {
        return Ok(Some(choices[uniform_index(rng, choices.len())]));
    }

    let mut total = 0.0;
    for edge in choices {
        if !edge.weight.is_finite() || edge.weight < 0.0 {
            return Err(AlgorithmError::Execution {
                message: "random-walk weights must be finite and nonnegative".into(),
            });
        }
        total += edge.weight;
        if !total.is_finite() {
            return Err(AlgorithmError::Execution {
                message: "random-walk weight sum must be finite".into(),
            });
        }
    }
    if total == 0.0 {
        return Ok(None);
    }
    let threshold = rng.unit_f64() * total;
    let mut cumulative = 0.0;
    for edge in choices {
        cumulative += edge.weight;
        if threshold < cumulative {
            return Ok(Some(edge));
        }
    }
    Ok(choices.last().copied())
}

fn derive_seed(seed: u64, source_ordinal: usize, walk_ordinal: usize) -> u64 {
    let source = u64::try_from(source_ordinal).unwrap_or(u64::MAX);
    let walk = u64::try_from(walk_ordinal).unwrap_or(u64::MAX);
    mix(seed ^ mix(source) ^ mix(walk.wrapping_add(0xD1B5_4A32_D192_ED03)))
}

fn uniform_index(rng: &mut SplitMix64, upper: usize) -> usize {
    let upper = u64::try_from(upper).expect("adjacency length fits u64");
    let threshold = upper.wrapping_neg() % upper;
    loop {
        let draw = rng.next();
        if draw >= threshold {
            return usize::try_from(draw % upper).expect("choice index fits usize");
        }
    }
}

#[derive(Clone, Copy)]
struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        mix(self.0)
    }

    fn unit_f64(&mut self) -> f64 {
        f64::from_bits(0x3FF0_0000_0000_0000 | (self.next() >> 12)) - 1.0
    }
}

fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmError, AlgorithmLimits};

    use super::*;

    fn uuid(value: u8) -> [u8; 16] {
        [value; 16]
    }

    fn edge(edge: u8, neighbor: u8, weight: f64) -> RandomWalkEdge {
        RandomWalkEdge {
            edge_uuid: uuid(edge),
            neighbor_uuid: uuid(neighbor),
            weight,
        }
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    #[test]
    fn seeded_walks_are_repeatable_and_ordered_by_source_then_ordinal() {
        let graph = HashMap::from([
            (uuid(0), vec![edge(2, 2, 1.0), edge(1, 1, 1.0)]),
            (uuid(1), vec![edge(3, 3, 1.0)]),
        ]);
        let sources = [uuid(0), uuid(2)];
        let first = random_walks(&graph, &sources, 2, 3, 42, false, &control()).unwrap();
        let second = random_walks(&graph, &sources, 2, 3, 42, false, &control()).unwrap();
        assert_eq!(first, second);
        assert_eq!(
            first,
            vec![
                vec![uuid(0), uuid(2)],
                vec![uuid(0), uuid(1), uuid(3)],
                vec![uuid(2)],
                vec![uuid(2)],
            ]
        );
        assert_eq!(RANDOM_WALK_RNG_CONTRACT, "splitmix64-v1");
    }

    #[test]
    fn zero_length_dead_ends_and_zero_total_weights_terminate_without_padding() {
        let graph = HashMap::from([
            (uuid(0), vec![edge(0, 1, 1.0)]),
            (uuid(2), vec![edge(1, 3, 0.0), edge(2, 4, 0.0)]),
        ]);
        assert_eq!(
            random_walks(&graph, &[uuid(0)], 1, 0, 1, false, &control()).unwrap(),
            vec![vec![uuid(0)]]
        );
        assert_eq!(
            random_walks(&graph, &[uuid(0)], 1, 4, 1, false, &control()).unwrap(),
            vec![vec![uuid(0), uuid(1)]]
        );
        assert_eq!(
            random_walks(&graph, &[uuid(2)], 1, 4, 1, true, &control()).unwrap(),
            vec![vec![uuid(2)]]
        );
    }

    #[test]
    fn weighted_parallel_edges_and_self_loops_are_choices_and_invalid_weights_fail() {
        let graph = HashMap::from([(
            uuid(0),
            vec![edge(2, 1, 1.0), edge(1, 1, 1.0), edge(3, 0, 1.0)],
        )]);
        assert_eq!(
            random_walks(&graph, &[uuid(0)], 1, 2, 7, true, &control()).unwrap(),
            vec![vec![uuid(0), uuid(1)]]
        );

        for weight in [f64::NAN, f64::INFINITY, -1.0] {
            let invalid = HashMap::from([(uuid(0), vec![edge(0, 1, weight)])]);
            assert!(matches!(
                random_walks(&invalid, &[uuid(0)], 1, 1, 0, true, &control()),
                Err(AlgorithmError::Execution { .. })
            ));
        }
    }

    #[test]
    fn cancellation_iteration_and_output_limits_fail_atomically() {
        let graph = HashMap::from([
            (uuid(0), vec![edge(0, 1, 1.0)]),
            (uuid(1), vec![edge(1, 0, 1.0)]),
        ]);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            random_walks(
                &graph,
                &[uuid(0)],
                1,
                1,
                0,
                false,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
        for (iterations, output_rows, expected) in [
            (1, u64::MAX, "iteration limit"),
            (u64::MAX, 1, "output row limit"),
        ] {
            let limits = AlgorithmLimits {
                iterations,
                output_rows,
                ..AlgorithmLimits::default()
            };
            let error = random_walks(
                &graph,
                &[uuid(0)],
                2,
                2,
                0,
                false,
                &AlgorithmControl::new(limits, AlgorithmCancellation::default()),
            )
            .unwrap_err();
            assert!(error.to_string().contains(expected));
        }
    }
}
