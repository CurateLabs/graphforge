//! Deterministic, dependency-free exact cosine K-nearest-neighbor kernel.

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const CHECKPOINT_INTERVAL: usize = 16_384;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct SimilarityPair {
    pub source_index: usize,
    pub target_index: usize,
    pub similarity: f64,
}

pub(crate) fn exact_cosine_knn(
    vectors: &[&[f64]],
    k: usize,
    control: &AlgorithmControl,
) -> Result<Vec<SimilarityPair>, AlgorithmError> {
    exact_cosine_pairs(vectors, k, false, control)
}

pub(crate) fn exact_cosine_similarity(
    vectors: &[&[f64]],
    k: usize,
    control: &AlgorithmControl,
) -> Result<Vec<SimilarityPair>, AlgorithmError> {
    exact_cosine_pairs(vectors, k, true, control)
}

fn exact_cosine_pairs(
    vectors: &[&[f64]],
    k: usize,
    include_negative: bool,
    control: &AlgorithmControl,
) -> Result<Vec<SimilarityPair>, AlgorithmError> {
    control.checkpoint()?;
    if k == 0 {
        return Err(execution("cosine k must be positive"));
    }
    if vectors.is_empty() {
        return Ok(Vec::new());
    }
    let norms = validate(vectors)?;
    let mut work = 0_usize;
    let mut pairs = Vec::new();

    for source_index in 0..vectors.len() {
        let mut candidates = Vec::with_capacity(vectors.len().saturating_sub(1));
        for target_index in 0..vectors.len() {
            if source_index == target_index {
                continue;
            }
            let similarity = cosine(
                vectors,
                &norms,
                source_index,
                target_index,
                control,
                &mut work,
            )?;
            if include_negative || similarity >= 0.0 {
                candidates.push((target_index, similarity));
            }
        }
        append_top_k(&mut pairs, source_index, candidates, k, control)?;
    }
    Ok(pairs)
}

pub(crate) fn exact_filtered_cosine_knn(
    vectors: &[&[f64]],
    candidate_indices: &[Vec<usize>],
    k: usize,
    control: &AlgorithmControl,
) -> Result<Vec<SimilarityPair>, AlgorithmError> {
    control.checkpoint()?;
    if k == 0 {
        return Err(execution("KNN k must be positive"));
    }
    if candidate_indices.len() != vectors.len() {
        return Err(execution(
            "filtered KNN candidate sets must match vector count",
        ));
    }
    if vectors.is_empty() {
        return Ok(Vec::new());
    }
    let norms = validate(vectors)?;
    let mut work = 0_usize;
    let mut pairs = Vec::new();

    for (source_index, source_candidates) in candidate_indices.iter().enumerate() {
        let mut seen = vec![false; vectors.len()];
        let mut candidates = Vec::with_capacity(source_candidates.len());
        for &target_index in source_candidates {
            checkpoint(control, &mut work)?;
            let Some(target_seen) = seen.get_mut(target_index) else {
                return Err(execution(
                    "filtered KNN candidate is outside vector selection",
                ));
            };
            if source_index == target_index || std::mem::replace(target_seen, true) {
                continue;
            }
            let similarity = cosine(
                vectors,
                &norms,
                source_index,
                target_index,
                control,
                &mut work,
            )?;
            if similarity >= 0.0 {
                candidates.push((target_index, similarity));
            }
        }
        append_top_k(&mut pairs, source_index, candidates, k, control)?;
    }
    Ok(pairs)
}

fn cosine(
    vectors: &[&[f64]],
    norms: &[f64],
    source_index: usize,
    target_index: usize,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<f64, AlgorithmError> {
    let mut dot = 0.0;
    for (&left, &right) in vectors[source_index].iter().zip(vectors[target_index]) {
        checkpoint(control, work)?;
        dot += left * right;
        if !dot.is_finite() {
            return Err(numeric_error());
        }
    }
    let similarity = (dot / (norms[source_index] * norms[target_index])).clamp(-1.0, 1.0);
    similarity
        .is_finite()
        .then_some(similarity)
        .ok_or_else(numeric_error)
}

fn append_top_k(
    pairs: &mut Vec<SimilarityPair>,
    source_index: usize,
    mut candidates: Vec<(usize, f64)>,
    k: usize,
    control: &AlgorithmControl,
) -> Result<(), AlgorithmError> {
    candidates.sort_by(|(left_index, left_score), (right_index, right_score)| {
        right_score
            .total_cmp(left_score)
            .then_with(|| left_index.cmp(right_index))
    });
    control.check_cancelled()?;
    for (target_index, similarity) in candidates.into_iter().take(k) {
        control.check_cancelled()?;
        control.check_output_rows(pairs.len().saturating_add(1))?;
        pairs.push(SimilarityPair {
            source_index,
            target_index,
            similarity,
        });
    }
    Ok(())
}

fn validate(vectors: &[&[f64]]) -> Result<Vec<f64>, AlgorithmError> {
    let dimension = vectors[0].len();
    if dimension == 0 {
        return Err(execution("KNN vectors must not be empty"));
    }
    vectors
        .iter()
        .map(|vector| {
            if vector.len() != dimension || vector.iter().any(|value| !value.is_finite()) {
                return Err(execution("KNN vectors must be finite and same-dimensional"));
            }
            let norm_squared = vector.iter().try_fold(0.0, |sum, value| {
                let next = sum + value * value;
                next.is_finite().then_some(next).ok_or_else(numeric_error)
            })?;
            if norm_squared == 0.0 {
                return Err(execution("KNN vectors must have non-zero norm"));
            }
            Ok(norm_squared.sqrt())
        })
        .collect()
}

fn checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    *work += 1;
    if (*work).is_multiple_of(CHECKPOINT_INTERVAL) {
        control.checkpoint()?;
    }
    Ok(())
}

fn numeric_error() -> AlgorithmError {
    execution("KNN numeric state is NaN or infinite")
}

fn execution(message: &str) -> AlgorithmError {
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
    fn exact_scores_cutoff_top_k_and_ties_are_stable() {
        let values = [
            vec![1.0, 0.0],
            vec![1.0, 0.0],
            vec![1.0, 1.0],
            vec![0.0, 1.0],
            vec![-1.0, 0.0],
        ];
        let vectors = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let first = exact_cosine_knn(&vectors, 2, &control()).unwrap();
        let second = exact_cosine_knn(&vectors, 2, &control()).unwrap();
        assert_eq!(first, second);
        assert_eq!(
            first
                .iter()
                .map(|pair| (pair.source_index, pair.target_index))
                .collect::<Vec<_>>(),
            [
                (0, 1),
                (0, 2),
                (1, 0),
                (1, 2),
                (2, 0),
                (2, 1),
                (3, 2),
                (3, 0),
                (4, 3)
            ]
        );
        assert_eq!(first[0].similarity, 1.0);
        assert!((first[1].similarity - 2.0_f64.sqrt().recip()).abs() < 1e-12);
        assert_eq!(first.last().unwrap().similarity, 0.0);
        assert!(first.iter().all(|pair| pair.similarity >= 0.0));
    }

    #[test]
    fn all_score_cosine_keeps_negative_zero_positive_and_stable_top_k() {
        let values = [
            vec![1.0, 0.0],
            vec![0.0, 1.0],
            vec![-1.0, 0.0],
            vec![-1.0, -1.0],
        ];
        let vectors = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let first = exact_cosine_similarity(&vectors, 3, &control()).unwrap();
        let second = exact_cosine_similarity(&vectors, 3, &control()).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.len(), 12);
        let source_zero = first
            .iter()
            .filter(|pair| pair.source_index == 0)
            .map(|pair| (pair.target_index, pair.similarity))
            .collect::<Vec<_>>();
        assert_eq!(source_zero[0], (1, 0.0));
        assert_eq!(source_zero[2], (2, -1.0));
        assert!((source_zero[1].1 + 2.0_f64.sqrt().recip()).abs() < 1e-12);
        assert_eq!(
            exact_cosine_similarity(&vectors, 2, &control())
                .unwrap()
                .len(),
            8
        );
        assert!(
            exact_cosine_similarity(&[], 1, &control())
                .unwrap()
                .is_empty()
        );
        assert!(
            exact_cosine_similarity(&[&[1.0]], 1, &control())
                .unwrap()
                .is_empty()
        );
        assert!(matches!(
            exact_cosine_similarity(&[&[1.0]], 0, &control()),
            Err(AlgorithmError::Execution { .. })
        ));
    }

    #[test]
    fn boundaries_and_invalid_vectors_are_structured() {
        assert!(exact_cosine_knn(&[], 1, &control()).unwrap().is_empty());
        assert!(
            exact_cosine_knn(&[&[1.0]], 1, &control())
                .unwrap()
                .is_empty()
        );
        for vectors in [
            vec![Vec::<f64>::new()],
            vec![vec![0.0, 0.0]],
            vec![vec![1.0], vec![1.0, 2.0]],
            vec![vec![f64::NAN]],
            vec![vec![f64::MAX, f64::MAX]],
        ] {
            let vectors = vectors.iter().map(Vec::as_slice).collect::<Vec<_>>();
            assert!(matches!(
                exact_cosine_knn(&vectors, 1, &control()),
                Err(AlgorithmError::Execution { .. })
            ));
        }
        assert!(matches!(
            exact_cosine_knn(&[&[1.0]], 0, &control()),
            Err(AlgorithmError::Execution { .. })
        ));
    }

    #[test]
    fn filtered_candidates_are_exact_stable_distinct_and_asymmetric() {
        let values = [
            vec![1.0, 0.0],
            vec![1.0, 0.0],
            vec![1.0, 1.0],
            vec![0.0, 1.0],
            vec![-1.0, 0.0],
        ];
        let vectors = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let candidates = [
            vec![4, 2, 1, 1, 0],
            vec![],
            vec![3, 1, 0],
            vec![4, 2, 0],
            vec![3],
        ];
        let first = exact_filtered_cosine_knn(&vectors, &candidates, 2, &control()).unwrap();
        let second = exact_filtered_cosine_knn(&vectors, &candidates, 2, &control()).unwrap();
        assert_eq!(first, second);
        assert_eq!(
            first
                .iter()
                .map(|pair| (pair.source_index, pair.target_index))
                .collect::<Vec<_>>(),
            [(0, 1), (0, 2), (2, 0), (2, 1), (3, 2), (3, 0), (4, 3)]
        );
        assert_eq!(first[0].similarity, 1.0);
        assert!((first[1].similarity - 2.0_f64.sqrt().recip()).abs() < 1e-12);
        assert_eq!(first.last().unwrap().similarity, 0.0);
    }

    #[test]
    fn filtered_boundaries_limits_and_cancellation_are_structured() {
        assert!(
            exact_filtered_cosine_knn(&[], &[], 1, &control())
                .unwrap()
                .is_empty()
        );
        assert!(
            exact_filtered_cosine_knn(&[&[1.0]], &[vec![0, 0]], 1, &control())
                .unwrap()
                .is_empty()
        );
        for (vectors, candidates) in [
            (vec![vec![1.0]], vec![]),
            (vec![vec![1.0]], vec![vec![1]]),
            (vec![Vec::<f64>::new()], vec![vec![]]),
            (vec![vec![0.0]], vec![vec![]]),
            (vec![vec![1.0], vec![1.0, 2.0]], vec![vec![], vec![]]),
            (vec![vec![f64::NAN]], vec![vec![]]),
            (vec![vec![f64::MAX, f64::MAX]], vec![vec![]]),
        ] {
            let vectors = vectors.iter().map(Vec::as_slice).collect::<Vec<_>>();
            assert!(matches!(
                exact_filtered_cosine_knn(&vectors, &candidates, 1, &control()),
                Err(AlgorithmError::Execution { .. })
            ));
        }
        assert!(matches!(
            exact_filtered_cosine_knn(&[&[1.0]], &[vec![]], 0, &control()),
            Err(AlgorithmError::Execution { .. })
        ));

        let vectors = [&[1.0][..], &[1.0][..]];
        let candidates = [vec![1], vec![0]];
        let limited = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            exact_filtered_cosine_knn(&vectors, &candidates, 1, &limited),
            Err(AlgorithmError::OutputLimit {
                observed: 2,
                limit: 1,
            })
        );
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let cancelled_control =
            AlgorithmControl::new(AlgorithmLimits::default(), cancellation.clone());
        assert_eq!(
            exact_filtered_cosine_knn(&vectors, &candidates, 1, &cancelled_control,),
            Err(AlgorithmError::Cancelled)
        );
        assert_eq!(
            append_top_k(&mut Vec::new(), 0, vec![(1, 1.0)], 1, &cancelled_control),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn output_limits_and_cancellation_stop_without_partial_results() {
        let values = [vec![1.0], vec![1.0]];
        let vectors = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let limited = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            exact_cosine_knn(&vectors, 1, &limited),
            Err(AlgorithmError::OutputLimit {
                observed: 2,
                limit: 1,
            })
        );
        assert_eq!(
            exact_cosine_similarity(&vectors, 1, &limited),
            Err(AlgorithmError::OutputLimit {
                observed: 2,
                limit: 1,
            })
        );

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let cancelled = AlgorithmControl::new(AlgorithmLimits::default(), cancellation);
        assert_eq!(
            exact_cosine_knn(&vectors, 1, &cancelled),
            Err(AlgorithmError::Cancelled)
        );
        assert_eq!(
            exact_cosine_similarity(&vectors, 1, &cancelled),
            Err(AlgorithmError::Cancelled)
        );
    }
}
