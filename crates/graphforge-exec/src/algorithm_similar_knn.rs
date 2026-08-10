//! Deterministic, dependency-free exact cosine K-nearest-neighbor kernel.
//!
//! Parallelism (#342) partitions work by canonical source ordinal through the
//! instance-owned private compute pool. Each source retains serial coordinate
//! order for validation, norms, every dot product, clamping, candidate
//! ordering, and top-k ties. Worker outputs merge in source order so results
//! stay bit-for-bit identical to the serial path.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};

use rayon::prelude::*;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const CHECKPOINT_INTERVAL: usize = 16_384;

/// Multiply-add ops below which cosine stays on the serial path (#342).
///
/// Measured to keep accepted small fixtures and micro-invocations off the
/// worker pool; above this, source-parallel execution amortizes scheduling on
/// typical embedded hosts. Exact numeric results remain identical either way.
pub const COSINE_PARALLEL_CROSSOVER_OPS: u64 = 16_384;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct SimilarityPair {
    pub source_index: usize,
    pub target_index: usize,
    pub similarity: f64,
}

/// Selected execution path for observability and crossover tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CosineExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
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
    let dimension = vectors[0].len();
    let sources = vectors.len();
    let candidates_per_source = sources.saturating_sub(1) as u64;
    let ops = estimated_ops(sources, candidates_per_source, dimension);
    match select_cosine_path(control, sources, ops) {
        CosineExecutionPath::Serial => {
            exact_cosine_pairs_serial(vectors, &norms, k, include_negative, control)
        }
        CosineExecutionPath::Parallel { .. } => {
            exact_cosine_pairs_parallel(vectors, &norms, k, include_negative, control)
        }
    }
}

fn exact_cosine_pairs_serial(
    vectors: &[&[f64]],
    norms: &[f64],
    k: usize,
    include_negative: bool,
    control: &AlgorithmControl,
) -> Result<Vec<SimilarityPair>, AlgorithmError> {
    let work = AtomicUsize::new(0);
    let mut pairs = Vec::new();
    for source_index in 0..vectors.len() {
        let source_pairs = score_source_all_pairs(
            vectors,
            norms,
            source_index,
            k,
            include_negative,
            control,
            &work,
        )?;
        append_checked(&mut pairs, source_pairs, control)?;
    }
    Ok(pairs)
}

fn exact_cosine_pairs_parallel(
    vectors: &[&[f64]],
    norms: &[f64],
    k: usize,
    include_negative: bool,
    control: &AlgorithmControl,
) -> Result<Vec<SimilarityPair>, AlgorithmError> {
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel cosine requires an instance-owned compute pool"))?;
    let ranges = source_chunks(vectors.len(), control.compute_threads());
    let work = AtomicUsize::new(0);
    let chunk_results = run_on_pool(pool, || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                let mut chunk_pairs = Vec::new();
                for source_index in start..end {
                    let source_pairs = score_source_all_pairs(
                        vectors,
                        norms,
                        source_index,
                        k,
                        include_negative,
                        control,
                        &work,
                    )?;
                    chunk_pairs.extend(source_pairs);
                }
                Ok(chunk_pairs)
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()
    })?;
    merge_chunk_pairs(chunk_results, control)
}

fn score_source_all_pairs(
    vectors: &[&[f64]],
    norms: &[f64],
    source_index: usize,
    k: usize,
    include_negative: bool,
    control: &AlgorithmControl,
    work: &AtomicUsize,
) -> Result<Vec<SimilarityPair>, AlgorithmError> {
    let mut candidates = Vec::with_capacity(vectors.len().saturating_sub(1));
    for target_index in 0..vectors.len() {
        if source_index == target_index {
            continue;
        }
        let similarity = cosine(vectors, norms, source_index, target_index, control, work)?;
        if include_negative || similarity >= 0.0 {
            candidates.push((target_index, similarity));
        }
    }
    top_k_pairs(source_index, candidates, k, control)
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
    let dimension = vectors[0].len();
    let sources = vectors.len();
    let candidate_comparisons = candidate_indices
        .iter()
        .map(Vec::len)
        .fold(0_u64, |acc, len| acc.saturating_add(len as u64));
    let ops = estimated_ops_from_comparisons(candidate_comparisons, dimension);
    match select_cosine_path(control, sources, ops) {
        CosineExecutionPath::Serial => {
            exact_filtered_cosine_knn_serial(vectors, &norms, candidate_indices, k, control)
        }
        CosineExecutionPath::Parallel { .. } => {
            exact_filtered_cosine_knn_parallel(vectors, &norms, candidate_indices, k, control)
        }
    }
}

fn exact_filtered_cosine_knn_serial(
    vectors: &[&[f64]],
    norms: &[f64],
    candidate_indices: &[Vec<usize>],
    k: usize,
    control: &AlgorithmControl,
) -> Result<Vec<SimilarityPair>, AlgorithmError> {
    let work = AtomicUsize::new(0);
    let mut pairs = Vec::new();
    for (source_index, source_candidates) in candidate_indices.iter().enumerate() {
        let source_pairs = score_source_filtered(
            vectors,
            norms,
            source_index,
            source_candidates,
            k,
            control,
            &work,
        )?;
        append_checked(&mut pairs, source_pairs, control)?;
    }
    Ok(pairs)
}

fn exact_filtered_cosine_knn_parallel(
    vectors: &[&[f64]],
    norms: &[f64],
    candidate_indices: &[Vec<usize>],
    k: usize,
    control: &AlgorithmControl,
) -> Result<Vec<SimilarityPair>, AlgorithmError> {
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel cosine requires an instance-owned compute pool"))?;
    let ranges = source_chunks(vectors.len(), control.compute_threads());
    let work = AtomicUsize::new(0);
    let chunk_results = run_on_pool(pool, || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                let mut chunk_pairs = Vec::new();
                for source_index in start..end {
                    let source_pairs = score_source_filtered(
                        vectors,
                        norms,
                        source_index,
                        &candidate_indices[source_index],
                        k,
                        control,
                        &work,
                    )?;
                    chunk_pairs.extend(source_pairs);
                }
                Ok(chunk_pairs)
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()
    })?;
    merge_chunk_pairs(chunk_results, control)
}

fn score_source_filtered(
    vectors: &[&[f64]],
    norms: &[f64],
    source_index: usize,
    source_candidates: &[usize],
    k: usize,
    control: &AlgorithmControl,
    work: &AtomicUsize,
) -> Result<Vec<SimilarityPair>, AlgorithmError> {
    let mut seen = vec![false; vectors.len()];
    let mut candidates = Vec::with_capacity(source_candidates.len());
    for &target_index in source_candidates {
        checkpoint(control, work)?;
        let Some(target_seen) = seen.get_mut(target_index) else {
            return Err(execution(
                "filtered KNN candidate is outside vector selection",
            ));
        };
        if source_index == target_index || std::mem::replace(target_seen, true) {
            continue;
        }
        let similarity = cosine(vectors, norms, source_index, target_index, control, work)?;
        if similarity >= 0.0 {
            candidates.push((target_index, similarity));
        }
    }
    top_k_pairs(source_index, candidates, k, control)
}

fn cosine(
    vectors: &[&[f64]],
    norms: &[f64],
    source_index: usize,
    target_index: usize,
    control: &AlgorithmControl,
    work: &AtomicUsize,
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

fn top_k_pairs(
    source_index: usize,
    mut candidates: Vec<(usize, f64)>,
    k: usize,
    control: &AlgorithmControl,
) -> Result<Vec<SimilarityPair>, AlgorithmError> {
    candidates.sort_by(|(left_index, left_score), (right_index, right_score)| {
        right_score
            .total_cmp(left_score)
            .then_with(|| left_index.cmp(right_index))
    });
    control.check_cancelled()?;
    let mut pairs = Vec::with_capacity(k.min(candidates.len()));
    for (target_index, similarity) in candidates.into_iter().take(k) {
        control.check_cancelled()?;
        pairs.push(SimilarityPair {
            source_index,
            target_index,
            similarity,
        });
    }
    Ok(pairs)
}

fn append_checked(
    pairs: &mut Vec<SimilarityPair>,
    source_pairs: Vec<SimilarityPair>,
    control: &AlgorithmControl,
) -> Result<(), AlgorithmError> {
    for pair in source_pairs {
        control.check_cancelled()?;
        control.check_output_rows(pairs.len().saturating_add(1))?;
        pairs.push(pair);
    }
    Ok(())
}

fn merge_chunk_pairs(
    chunk_results: Vec<Vec<SimilarityPair>>,
    control: &AlgorithmControl,
) -> Result<Vec<SimilarityPair>, AlgorithmError> {
    let mut pairs = Vec::new();
    for chunk in chunk_results {
        append_checked(&mut pairs, chunk, control)?;
    }
    Ok(pairs)
}

fn run_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("cosine worker panicked")),
    }
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

fn checkpoint(control: &AlgorithmControl, work: &AtomicUsize) -> Result<(), AlgorithmError> {
    let observed = work.fetch_add(1, Ordering::Relaxed) + 1;
    if observed.is_multiple_of(CHECKPOINT_INTERVAL) {
        control.checkpoint()?;
    }
    Ok(())
}

fn estimated_ops(sources: usize, candidates_per_source: u64, dimension: usize) -> u64 {
    estimated_ops_from_comparisons(
        (sources as u64).saturating_mul(candidates_per_source),
        dimension,
    )
}

fn estimated_ops_from_comparisons(comparisons: u64, dimension: usize) -> u64 {
    comparisons.saturating_mul(dimension as u64)
}

/// Choose serial vs private-pool parallel execution for a cosine workload.
pub(crate) fn select_cosine_path(
    control: &AlgorithmControl,
    sources: usize,
    estimated_ops: u64,
) -> CosineExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1 || sources <= 1 || estimated_ops < COSINE_PARALLEL_CROSSOVER_OPS {
        return CosineExecutionPath::Serial;
    }
    if control
        .compute_pool()
        .is_none_or(|pool| !pool.is_parallel())
    {
        return CosineExecutionPath::Serial;
    }
    let chunks = source_chunks(sources, threads).len();
    if chunks <= 1 {
        return CosineExecutionPath::Serial;
    }
    CosineExecutionPath::Parallel { threads, chunks }
}

fn source_chunks(sources: usize, threads: usize) -> Vec<(usize, usize)> {
    if sources == 0 {
        return Vec::new();
    }
    let workers = threads.clamp(1, sources);
    let base = sources / workers;
    let rem = sources % workers;
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
    use crate::compute_pool::ComputePool;
    use std::sync::Arc;

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn control_with_threads(threads: usize) -> AlgorithmControl {
        let pool = Arc::new(ComputePool::new(threads).unwrap());
        AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(threads),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(pool)
    }

    fn fingerprint(pairs: &[SimilarityPair]) -> Vec<(usize, usize, u64)> {
        pairs
            .iter()
            .map(|pair| {
                (
                    pair.source_index,
                    pair.target_index,
                    pair.similarity.to_bits(),
                )
            })
            .collect()
    }

    fn adversarial_vectors(count: usize, dimension: usize) -> Vec<Vec<f64>> {
        (0..count)
            .map(|source| {
                (0..dimension)
                    .map(|axis| {
                        let signed = if (source + axis) % 2 == 0 { 1.0 } else { -1.0 };
                        signed * ((source * 17 + axis * 13) % 97 + 1) as f64 / 97.0
                    })
                    .collect()
            })
            .collect()
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
            top_k_pairs(0, vec![(1, 1.0)], 1, &cancelled_control),
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

    #[test]
    fn small_work_and_one_thread_select_serial_path() {
        let small = select_cosine_path(&control_with_threads(4), 4, 64);
        assert_eq!(small, CosineExecutionPath::Serial);
        let one = select_cosine_path(&control_with_threads(1), 64, COSINE_PARALLEL_CROSSOVER_OPS);
        assert_eq!(one, CosineExecutionPath::Serial);
        let large = select_cosine_path(&control_with_threads(4), 64, COSINE_PARALLEL_CROSSOVER_OPS);
        assert!(matches!(
            large,
            CosineExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        ));
    }

    #[test]
    fn source_chunks_cover_canonical_ranges() {
        assert_eq!(source_chunks(0, 4), Vec::<(usize, usize)>::new());
        assert_eq!(source_chunks(5, 1), vec![(0, 5)]);
        assert_eq!(source_chunks(5, 2), vec![(0, 3), (3, 5)]);
        assert_eq!(source_chunks(8, 4), vec![(0, 2), (2, 4), (4, 6), (6, 8)]);
        assert_eq!(source_chunks(3, 8), vec![(0, 1), (1, 2), (2, 3)]);
    }

    #[test]
    fn thread_matrix_preserves_exact_knn_cosine_and_filtered_fingerprints() {
        let values = adversarial_vectors(48, 16);
        let vectors = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let candidates = (0..values.len())
            .map(|source| {
                (0..values.len())
                    .filter(|&target| target != source && (source + target) % 3 != 0)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let serial = control_with_threads(1);
        let knn_serial = fingerprint(&exact_cosine_knn(&vectors, 5, &serial).unwrap());
        let cosine_serial = fingerprint(&exact_cosine_similarity(&vectors, 7, &serial).unwrap());
        let filtered_serial =
            fingerprint(&exact_filtered_cosine_knn(&vectors, &candidates, 4, &serial).unwrap());
        for threads in [2_usize, 4, 8] {
            let parallel = control_with_threads(threads);
            assert!(matches!(
                select_cosine_path(
                    &parallel,
                    values.len(),
                    estimated_ops(values.len(), (values.len() - 1) as u64, 16)
                ),
                CosineExecutionPath::Parallel { .. }
            ));
            assert_eq!(
                fingerprint(&exact_cosine_knn(&vectors, 5, &parallel).unwrap()),
                knn_serial
            );
            assert_eq!(
                fingerprint(&exact_cosine_similarity(&vectors, 7, &parallel).unwrap()),
                cosine_serial
            );
            assert_eq!(
                fingerprint(
                    &exact_filtered_cosine_knn(&vectors, &candidates, 4, &parallel).unwrap()
                ),
                filtered_serial
            );
        }
    }

    #[test]
    fn parallel_output_limits_and_cancellation_remain_atomic() {
        let values = adversarial_vectors(32, 8);
        let vectors = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let pool = Arc::new(ComputePool::new(4).unwrap());
        let limited = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 3,
                compute_threads: 4,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(pool.clone());
        assert_eq!(
            exact_cosine_knn(&vectors, 2, &limited),
            Err(AlgorithmError::OutputLimit {
                observed: 4,
                limit: 3,
            })
        );

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let cancelled = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            cancellation,
        )
        .with_compute_pool(pool);
        assert_eq!(
            exact_cosine_similarity(&vectors, 3, &cancelled),
            Err(AlgorithmError::Cancelled)
        );
    }
}
