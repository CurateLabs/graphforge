//! Deterministic, dependency-free exact Jaccard similarity kernel.

use std::collections::HashSet;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const CHECKPOINT_INTERVAL: usize = 16_384;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct JaccardPair {
    pub source_index: usize,
    pub target_index: usize,
    pub similarity: f64,
}

pub(crate) fn exact_jaccard(
    neighborhoods: &[HashSet<u64>],
    candidate_indices: Option<&[Vec<usize>]>,
    k: usize,
    control: &AlgorithmControl,
) -> Result<Vec<JaccardPair>, AlgorithmError> {
    if k == 0 {
        return Err(execution("node similarity k must be positive"));
    }
    if candidate_indices.is_some_and(|candidates| candidates.len() != neighborhoods.len()) {
        return Err(execution("candidate sets must match neighborhood count"));
    }

    let mut work = 0_usize;
    let mut pairs = Vec::new();
    for (source_index, source) in neighborhoods.iter().enumerate() {
        if source.is_empty() {
            continue;
        }
        let mut seen = vec![false; neighborhoods.len()];
        let candidates: Box<dyn Iterator<Item = usize>> = match candidate_indices {
            Some(filtered) => Box::new(filtered[source_index].iter().copied()),
            None => Box::new(0..neighborhoods.len()),
        };
        let mut scores = Vec::new();
        for target_index in candidates {
            checkpoint(control, &mut work)?;
            let Some(target_seen) = seen.get_mut(target_index) else {
                return Err(execution("candidate is outside node selection"));
            };
            if source_index == target_index || std::mem::replace(target_seen, true) {
                continue;
            }
            let target = &neighborhoods[target_index];
            if target.is_empty() {
                continue;
            }
            let intersection = source.intersection(target).count();
            if intersection == 0 {
                continue;
            }
            let union = source.len() + target.len() - intersection;
            let intersection = exact_u32(intersection, "neighbor intersection")?;
            let union = exact_u32(union, "neighbor union")?;
            scores.push((target_index, f64::from(intersection) / f64::from(union)));
        }
        scores.sort_by(|(left_index, left_score), (right_index, right_score)| {
            right_score
                .total_cmp(left_score)
                .then_with(|| left_index.cmp(right_index))
        });
        control.check_cancelled()?;
        for (target_index, similarity) in scores.into_iter().take(k) {
            control.check_cancelled()?;
            control.check_output_rows(pairs.len().saturating_add(1))?;
            pairs.push(JaccardPair {
                source_index,
                target_index,
                similarity,
            });
        }
    }
    Ok(pairs)
}

fn checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    if (*work).is_multiple_of(CHECKPOINT_INTERVAL) {
        control.checkpoint()?;
    }
    *work += 1;
    Ok(())
}

fn exact_u32(value: usize, kind: &str) -> Result<u32, AlgorithmError> {
    u32::try_from(value).map_err(|_| execution(format!("{kind} exceeds supported score range")))
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

    fn set(values: &[u64]) -> HashSet<u64> {
        values.iter().copied().collect()
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    #[test]
    fn filtered_candidates_are_exact_deduplicated_asymmetric_and_stable() {
        let neighborhoods = [set(&[1, 2, 3]), set(&[0, 2, 3]), set(&[3]), set(&[2])];
        let candidates = vec![vec![0, 1, 2, 2, 3], vec![0, 2, 3], vec![3], vec![2]];
        let first = exact_jaccard(&neighborhoods, Some(&candidates), 2, &control()).unwrap();
        let second = exact_jaccard(&neighborhoods, Some(&candidates), 2, &control()).unwrap();
        assert_eq!(first, second);
        assert_eq!(
            first,
            vec![
                JaccardPair {
                    source_index: 0,
                    target_index: 1,
                    similarity: 0.5,
                },
                JaccardPair {
                    source_index: 0,
                    target_index: 2,
                    similarity: 1.0 / 3.0,
                },
                JaccardPair {
                    source_index: 1,
                    target_index: 0,
                    similarity: 0.5,
                },
                JaccardPair {
                    source_index: 1,
                    target_index: 2,
                    similarity: 1.0 / 3.0,
                },
            ]
        );

        let self_loop_neighborhood = [set(&[0, 2]), set(&[0])];
        let reciprocal = vec![vec![1], vec![0]];
        assert_eq!(
            exact_jaccard(&self_loop_neighborhood, Some(&reciprocal), 1, &control()).unwrap(),
            vec![
                JaccardPair {
                    source_index: 0,
                    target_index: 1,
                    similarity: 0.5,
                },
                JaccardPair {
                    source_index: 1,
                    target_index: 0,
                    similarity: 0.5,
                },
            ]
        );
    }

    #[test]
    fn boundaries_limits_and_cancellation_are_structured() {
        assert!(
            exact_jaccard(&[], Some(&[]), 1, &control())
                .unwrap()
                .is_empty()
        );
        assert!(
            exact_jaccard(&[set(&[0])], Some(&[vec![0]]), 1, &control())
                .unwrap()
                .is_empty()
        );
        assert!(
            exact_jaccard(
                &[set(&[]), set(&[0])],
                Some(&[vec![], vec![0]]),
                1,
                &control()
            )
            .unwrap()
            .is_empty()
        );
        assert!(matches!(
            exact_jaccard(&[set(&[1])], Some(&[]), 1, &control()),
            Err(AlgorithmError::Execution { .. })
        ));
        assert!(matches!(
            exact_jaccard(&[set(&[1])], Some(&[vec![1]]), 1, &control()),
            Err(AlgorithmError::Execution { .. })
        ));
        assert!(matches!(
            exact_jaccard(&[set(&[1]), set(&[1])], None, 0, &control()),
            Err(AlgorithmError::Execution { .. })
        ));

        let neighborhoods = [set(&[2]), set(&[2])];
        let limited = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            exact_jaccard(&neighborhoods, None, 2, &limited),
            Err(AlgorithmError::OutputLimit {
                observed: 2,
                limit: 1
            })
        ));

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let cancelled = AlgorithmControl::new(AlgorithmLimits::default(), cancellation);
        assert_eq!(
            exact_jaccard(&neighborhoods, None, 1, &cancelled),
            Err(AlgorithmError::Cancelled)
        );

        let no_iterations = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            exact_jaccard(&neighborhoods, None, 1, &no_iterations),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0,
            })
        );
    }
}
