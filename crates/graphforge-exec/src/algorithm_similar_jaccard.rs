//! Deterministic exact Jaccard similarity kernel.
//!
//! Parallelism (#535) partitions work by canonical source ordinal through the
//! instance-owned private compute pool. Each source retains serial candidate
//! order for validation, deduplication, intersection counting, candidate
//! ordering, and top-k ties. Worker outputs merge in source order so results
//! stay bit-for-bit identical to the serial path.

use std::collections::HashSet;
use std::panic::{AssertUnwindSafe, catch_unwind};

use rayon::prelude::*;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const CHECKPOINT_INTERVAL: usize = 16_384;

/// Source-degree candidate probes below which Jaccard stays on the serial path (#535).
///
/// Chosen from release-mode serial-vs-parallel timings of exact Jaccard on this
/// M4 agent host (4× Xeon vCPU, adversarial set fixture, 4 private workers; see
/// ignored `measure_jaccard_parallel_crossover`):
/// - ~32k probes: parallel still slower (Rayon install + merge tax)
/// - ~62k probes: first clear win (~0.78× serial)
/// - ≥130k probes: ≥2× speedup
///
/// `65_536` is the smallest power-of-two at/above that measured win boundary.
/// Exact numeric results remain identical on either path.
pub const JACCARD_PARALLEL_CROSSOVER_OPS: u64 = 65_536;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct JaccardPair {
    pub source_index: usize,
    pub target_index: usize,
    pub similarity: f64,
}

/// Selected execution path for observability and crossover tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JaccardExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
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

    let estimated_ops = estimated_jaccard_ops(neighborhoods, candidate_indices);
    match select_jaccard_path(control, neighborhoods.len(), estimated_ops) {
        JaccardExecutionPath::Serial => {
            exact_jaccard_serial(neighborhoods, candidate_indices, k, control)
        }
        JaccardExecutionPath::Parallel { .. } => {
            exact_jaccard_parallel(neighborhoods, candidate_indices, k, control)
        }
    }
}

fn exact_jaccard_serial(
    neighborhoods: &[HashSet<u64>],
    candidate_indices: Option<&[Vec<usize>]>,
    k: usize,
    control: &AlgorithmControl,
) -> Result<Vec<JaccardPair>, AlgorithmError> {
    let mut work = 0_usize;
    let mut pairs = Vec::new();
    for source_index in 0..neighborhoods.len() {
        let source_pairs = score_source(
            neighborhoods,
            candidate_indices,
            source_index,
            k,
            control,
            &mut work,
        )?;
        append_checked(&mut pairs, source_pairs, control)?;
    }
    Ok(pairs)
}

fn exact_jaccard_parallel(
    neighborhoods: &[HashSet<u64>],
    candidate_indices: Option<&[Vec<usize>]>,
    k: usize,
    control: &AlgorithmControl,
) -> Result<Vec<JaccardPair>, AlgorithmError> {
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel Jaccard requires an instance-owned compute pool"))?;
    let ranges = source_chunks(neighborhoods.len(), control.compute_threads());
    let chunk_results = run_on_pool(pool, || {
        let results = ranges
            .par_iter()
            .map(|&(start, end)| {
                let mut work = 0usize;
                let mut chunk_pairs = Vec::new();
                for source_index in start..end {
                    let source_pairs = score_source(
                        neighborhoods,
                        candidate_indices,
                        source_index,
                        k,
                        control,
                        &mut work,
                    )?;
                    chunk_pairs.extend(source_pairs);
                }
                Ok(chunk_pairs)
            })
            .collect::<Vec<Result<_, AlgorithmError>>>();
        first_chunk_error(results)
    })?;
    merge_chunk_pairs(chunk_results, control)
}

fn score_source(
    neighborhoods: &[HashSet<u64>],
    candidate_indices: Option<&[Vec<usize>]>,
    source_index: usize,
    k: usize,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<JaccardPair>, AlgorithmError> {
    let source = &neighborhoods[source_index];
    if source.is_empty() {
        return Ok(Vec::new());
    }
    match candidate_indices {
        Some(filtered) => score_source_candidates(
            neighborhoods,
            source_index,
            filtered[source_index].iter().copied(),
            k,
            control,
            work,
        ),
        None => score_source_candidates(
            neighborhoods,
            source_index,
            0..neighborhoods.len(),
            k,
            control,
            work,
        ),
    }
}

fn score_source_candidates(
    neighborhoods: &[HashSet<u64>],
    source_index: usize,
    candidates: impl IntoIterator<Item = usize>,
    k: usize,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<JaccardPair>, AlgorithmError> {
    let source = &neighborhoods[source_index];
    let mut seen = vec![false; neighborhoods.len()];
    let mut scores = Vec::new();
    for target_index in candidates {
        checkpoint(control, work)?;
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
    top_k_pairs(source_index, scores, k, control)
}

fn top_k_pairs(
    source_index: usize,
    mut scores: Vec<(usize, f64)>,
    k: usize,
    control: &AlgorithmControl,
) -> Result<Vec<JaccardPair>, AlgorithmError> {
    scores.sort_by(|(left_index, left_score), (right_index, right_score)| {
        right_score
            .total_cmp(left_score)
            .then_with(|| left_index.cmp(right_index))
    });
    control.check_cancelled()?;
    let mut pairs = Vec::with_capacity(k.min(scores.len()));
    for (target_index, similarity) in scores.into_iter().take(k) {
        control.check_cancelled()?;
        pairs.push(JaccardPair {
            source_index,
            target_index,
            similarity,
        });
    }
    Ok(pairs)
}

fn append_checked(
    pairs: &mut Vec<JaccardPair>,
    source_pairs: Vec<JaccardPair>,
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
    chunk_results: Vec<Vec<JaccardPair>>,
    control: &AlgorithmControl,
) -> Result<Vec<JaccardPair>, AlgorithmError> {
    let mut pairs = Vec::new();
    for chunk in chunk_results {
        append_checked(&mut pairs, chunk, control)?;
    }
    Ok(pairs)
}

/// Prefer the lowest-index chunk error so parallel failures stay deterministic.
fn first_chunk_error<T>(results: Vec<Result<T, AlgorithmError>>) -> Result<Vec<T>, AlgorithmError> {
    let mut ok = Vec::with_capacity(results.len());
    let mut first_error: Option<AlgorithmError> = None;
    for result in results {
        match result {
            Ok(value) if first_error.is_none() => ok.push(value),
            Err(error) if first_error.is_none() => first_error = Some(error),
            Ok(_) | Err(_) => {}
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(ok),
    }
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
        Err(_) => Err(execution("Jaccard worker panicked")),
    }
}

fn checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    if (*work).is_multiple_of(CHECKPOINT_INTERVAL) {
        control.checkpoint()?;
    }
    *work += 1;
    Ok(())
}

fn estimated_jaccard_ops(
    neighborhoods: &[HashSet<u64>],
    candidate_indices: Option<&[Vec<usize>]>,
) -> u64 {
    neighborhoods
        .iter()
        .enumerate()
        .map(|(source_index, source)| {
            if source.is_empty() {
                return 0;
            }
            let candidates = candidate_indices
                .map_or(neighborhoods.len(), |filtered| filtered[source_index].len());
            (source.len() as u64).saturating_mul(candidates as u64)
        })
        .fold(0_u64, u64::saturating_add)
}

/// Choose serial vs private-pool parallel execution for a Jaccard workload.
pub(crate) fn select_jaccard_path(
    control: &AlgorithmControl,
    sources: usize,
    estimated_ops: u64,
) -> JaccardExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1 || sources <= 1 || estimated_ops < JACCARD_PARALLEL_CROSSOVER_OPS {
        return JaccardExecutionPath::Serial;
    }
    if control
        .compute_pool()
        .is_none_or(|pool| !pool.is_parallel())
    {
        return JaccardExecutionPath::Serial;
    }
    let chunks = source_chunks(sources, threads).len();
    if chunks <= 1 {
        return JaccardExecutionPath::Serial;
    }
    JaccardExecutionPath::Parallel { threads, chunks }
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
    use crate::compute_pool::ComputePool;
    use std::sync::Arc;

    fn set(values: &[u64]) -> HashSet<u64> {
        values.iter().copied().collect()
    }

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

    fn fingerprint(pairs: &[JaccardPair]) -> Vec<(usize, usize, u64)> {
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

    fn adversarial_neighborhoods(count: usize, degree: usize) -> Vec<HashSet<u64>> {
        let universe = (count * degree / 2).max(1) as u64;
        (0..count)
            .map(|source| {
                let mut values = HashSet::with_capacity(degree + 1);
                values.insert(0);
                for offset in 0..degree {
                    values.insert(((source * 17 + offset * 13) as u64 % universe) + 1);
                }
                values
            })
            .collect()
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

    #[test]
    fn small_work_and_one_thread_select_serial_path() {
        let small = select_jaccard_path(&control_with_threads(4), 4, 64);
        assert_eq!(small, JaccardExecutionPath::Serial);
        let one = select_jaccard_path(&control_with_threads(1), 64, JACCARD_PARALLEL_CROSSOVER_OPS);
        assert_eq!(one, JaccardExecutionPath::Serial);
        let large =
            select_jaccard_path(&control_with_threads(4), 64, JACCARD_PARALLEL_CROSSOVER_OPS);
        assert!(matches!(
            large,
            JaccardExecutionPath::Parallel {
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
    fn thread_matrix_preserves_exact_and_filtered_jaccard_fingerprints() {
        // 64×64×33 ≈ 135k probes exceeds JACCARD_PARALLEL_CROSSOVER_OPS.
        let neighborhoods = adversarial_neighborhoods(64, 32);
        let candidates = (0..neighborhoods.len())
            .map(|source| {
                (0..neighborhoods.len())
                    .filter(|&target| target != source && (source + target) % 3 != 0)
                    .flat_map(|target| [target, target])
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let serial = control_with_threads(1);
        let all_serial = fingerprint(&exact_jaccard(&neighborhoods, None, 5, &serial).unwrap());
        let filtered_serial =
            fingerprint(&exact_jaccard(&neighborhoods, Some(&candidates), 4, &serial).unwrap());
        for threads in [2_usize, 4, 8] {
            let parallel = control_with_threads(threads);
            assert!(matches!(
                select_jaccard_path(
                    &parallel,
                    neighborhoods.len(),
                    estimated_jaccard_ops(&neighborhoods, None)
                ),
                JaccardExecutionPath::Parallel { .. }
            ));
            assert_eq!(
                fingerprint(&exact_jaccard(&neighborhoods, None, 5, &parallel).unwrap()),
                all_serial
            );
            assert_eq!(
                fingerprint(
                    &exact_jaccard(&neighborhoods, Some(&candidates), 4, &parallel).unwrap()
                ),
                filtered_serial
            );
        }
    }

    #[test]
    fn parallel_output_limits_and_cancellation_remain_atomic() {
        let neighborhoods = adversarial_neighborhoods(64, 32);
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
        assert!(matches!(
            select_jaccard_path(
                &limited,
                neighborhoods.len(),
                estimated_jaccard_ops(&neighborhoods, None)
            ),
            JaccardExecutionPath::Parallel { .. }
        ));
        assert_eq!(
            exact_jaccard(&neighborhoods, None, 2, &limited),
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
            exact_jaccard(&neighborhoods, None, 3, &cancelled),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn first_chunk_error_prefers_lowest_index() {
        let results = vec![
            Err(AlgorithmError::Cancelled),
            Err(AlgorithmError::OutputLimit {
                observed: 9,
                limit: 1,
            }),
            Ok(vec![JaccardPair {
                source_index: 0,
                target_index: 1,
                similarity: 1.0,
            }]),
        ];
        assert_eq!(first_chunk_error(results), Err(AlgorithmError::Cancelled));
    }

    #[test]
    #[ignore = "manual crossover measurement; run with --ignored --nocapture"]
    fn measure_jaccard_parallel_crossover() {
        use std::time::Instant;
        let pool = Arc::new(ComputePool::new(4).unwrap());
        for &(sources, degree) in &[
            (16usize, 16usize),
            (32, 16),
            (48, 20),
            (64, 24),
            (64, 32),
            (96, 32),
            (128, 32),
            (256, 48),
            (512, 64),
        ] {
            let neighborhoods = adversarial_neighborhoods(sources, degree);
            let ops = estimated_jaccard_ops(&neighborhoods, None);
            let limits = AlgorithmLimits {
                iterations: u64::MAX,
                ..AlgorithmLimits::default()
            };
            let serial_ctl = AlgorithmControl::new(
                limits.with_compute_threads(1),
                AlgorithmCancellation::default(),
            );
            let parallel_ctl = AlgorithmControl::new(
                limits.with_compute_threads(4),
                AlgorithmCancellation::default(),
            )
            .with_compute_pool(pool.clone());
            // Warm once.
            let _ = exact_jaccard_serial(&neighborhoods, None, 5, &serial_ctl).unwrap();
            let _ = exact_jaccard_parallel(&neighborhoods, None, 5, &parallel_ctl).unwrap();
            let mut serial_ns = u128::MAX;
            let mut parallel_ns = u128::MAX;
            for _ in 0..5 {
                let t0 = Instant::now();
                let a = exact_jaccard_serial(&neighborhoods, None, 5, &serial_ctl).unwrap();
                serial_ns = serial_ns.min(t0.elapsed().as_nanos());
                let t1 = Instant::now();
                let b = exact_jaccard_parallel(&neighborhoods, None, 5, &parallel_ctl).unwrap();
                parallel_ns = parallel_ns.min(t1.elapsed().as_nanos());
                assert_eq!(fingerprint(&a), fingerprint(&b));
            }
            eprintln!(
                "sources={sources} degree={degree} ops={ops} serial_ns={serial_ns} parallel_ns={parallel_ns} ratio={}",
                parallel_ns as f64 / serial_ns as f64
            );
        }
    }
}
