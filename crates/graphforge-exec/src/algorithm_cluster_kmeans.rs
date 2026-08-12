//! Deterministic, dependency-free K-means kernel for vector clustering.
//!
//! Assignment (#524) may partition independent point-to-centroid searches across
//! the instance-owned private compute pool above a documented crossover.
//! Farthest-first initialization and centroid updates remain serial, preserving
//! topology ties and floating-point accumulation order.
use std::panic::{AssertUnwindSafe, catch_unwind};

use rayon::prelude::*;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

pub(crate) const CLUSTER_COUNT: usize = 10;
const MAX_ITERATIONS: usize = 100;
const CHECKPOINT_INTERVAL: usize = 64;

/// Distance-coordinate evaluations below which assignment stays serial (#524).
///
/// The unit is `points * CLUSTER_COUNT * dimensions`; below this threshold,
/// private-pool scheduling and merge overhead dominate the independent assign
/// work. Distance coordinates remain serial for each point/centroid pair.
pub const KMEANS_ASSIGN_PARALLEL_CROSSOVER_OPS: u64 = 32_768;

/// Selected assignment execution path for observability and crossover tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KMeansAssignPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

pub(crate) fn stable_labels(
    vectors: &[&[f64]],
    control: &AlgorithmControl,
) -> Result<Vec<i64>, AlgorithmError> {
    fit(vectors, control, MAX_ITERATIONS)
}

fn fit(
    vectors: &[&[f64]],
    control: &AlgorithmControl,
    max_iterations: usize,
) -> Result<Vec<i64>, AlgorithmError> {
    control.checkpoint()?;
    if vectors.is_empty() {
        return Ok(Vec::new());
    }
    validate(vectors)?;
    let mut work = 0;
    let mut centroids = initialize(vectors, control, &mut work)?;
    let mut previous = vec![usize::MAX; vectors.len()];
    for _ in 0..max_iterations {
        control.checkpoint()?;
        let assignments = assign(vectors, &centroids, control, &mut work)?;
        if assignments == previous {
            return canonical_labels(&assignments);
        }
        centroids = update(vectors, &assignments, centroids, control, &mut work)?;
        previous = assignments;
    }
    Err(AlgorithmError::NonConvergence {
        iterations: max_iterations as u64,
    })
}

fn validate(vectors: &[&[f64]]) -> Result<(), AlgorithmError> {
    if vectors.len() < CLUSTER_COUNT {
        return Err(execution("K-means requires at least 10 selected nodes"));
    }
    let dimension = vectors[0].len();
    if dimension == 0
        || vectors.iter().any(|vector| {
            vector.len() != dimension || vector.iter().any(|value| !value.is_finite())
        })
    {
        return Err(execution(
            "K-means vectors must be non-empty, finite, and same-dimensional",
        ));
    }
    Ok(())
}

fn initialize(
    vectors: &[&[f64]],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<Vec<f64>>, AlgorithmError> {
    let mut selected = vec![false; vectors.len()];
    selected[0] = true;
    let mut indices = vec![0];
    while indices.len() < CLUSTER_COUNT {
        let mut best: Option<(f64, usize)> = None;
        for point in 0..vectors.len() {
            checkpoint(control, work)?;
            if selected[point] {
                continue;
            }
            let nearest = indices
                .iter()
                .try_fold(f64::INFINITY, |nearest, &centroid| {
                    squared_distance(vectors[point], vectors[centroid], control, work)
                        .map(|distance| nearest.min(distance))
                })?;
            if best.is_none_or(|(distance, index)| {
                nearest
                    .total_cmp(&distance)
                    .then_with(|| index.cmp(&point))
                    .is_gt()
            }) {
                best = Some((nearest, point));
            }
        }
        let point = best
            .map(|(_, point)| point)
            .ok_or_else(|| execution("K-means centroid initialization exhausted points"))?;
        selected[point] = true;
        indices.push(point);
    }
    Ok(indices
        .into_iter()
        .map(|index| vectors[index].to_vec())
        .collect())
}
fn assign(
    vectors: &[&[f64]],
    centroids: &[Vec<f64>],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<usize>, AlgorithmError> {
    let dimension = vectors.first().map_or(0, |vector| vector.len());
    match select_kmeans_assign_path(control, vectors.len(), dimension) {
        KMeansAssignPath::Serial => assign_serial(vectors, centroids, control, work),
        KMeansAssignPath::Parallel { .. } => assign_parallel(vectors, centroids, control),
    }
}

fn assign_serial(
    vectors: &[&[f64]],
    centroids: &[Vec<f64>],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<usize>, AlgorithmError> {
    vectors
        .iter()
        .map(|vector| {
            centroids
                .iter()
                .enumerate()
                .map(|(index, centroid)| {
                    squared_distance(vector, centroid, control, work)
                        .map(|distance| (distance, index))
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .min_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)))
                .map(|(_, index)| index)
                .ok_or_else(|| execution("K-means has no centroid"))
        })
        .collect()
}

fn assign_parallel(
    vectors: &[&[f64]],
    centroids: &[Vec<f64>],
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    let pool = control.compute_pool().ok_or_else(|| {
        execution("parallel K-means assignment requires an instance-owned compute pool")
    })?;
    let ranges = point_chunks(vectors.len(), control.compute_threads());
    let chunk_results = run_on_pool(pool, || {
        let results = ranges
            .par_iter()
            .map(|&(start, end)| {
                let mut work = 0_usize;
                let mut chunk = Vec::with_capacity(end.saturating_sub(start));
                for vector in &vectors[start..end] {
                    chunk.push(assign_one(vector, centroids, control, &mut work)?);
                }
                Ok(chunk)
            })
            .collect::<Vec<Result<Vec<usize>, AlgorithmError>>>();
        first_chunk_error(results)
    })?;
    Ok(chunk_results.into_iter().flatten().collect())
}

fn assign_one(
    vector: &[f64],
    centroids: &[Vec<f64>],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<usize, AlgorithmError> {
    centroids
        .iter()
        .enumerate()
        .map(|(index, centroid)| {
            squared_distance(vector, centroid, control, work).map(|distance| (distance, index))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .min_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)))
        .map(|(_, index)| index)
        .ok_or_else(|| execution("K-means has no centroid"))
}

fn update(
    vectors: &[&[f64]],
    assignments: &[usize],
    mut centroids: Vec<Vec<f64>>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<Vec<f64>>, AlgorithmError> {
    let dimension = vectors[0].len();
    let mut sums = vec![vec![0.0; dimension]; CLUSTER_COUNT];
    let mut counts = [0_u32; CLUSTER_COUNT];
    for (vector, &cluster) in vectors.iter().zip(assignments) {
        counts[cluster] = counts[cluster]
            .checked_add(1)
            .ok_or_else(|| execution("K-means cluster size exceeds UInt32"))?;
        for (sum, &value) in sums[cluster].iter_mut().zip(*vector) {
            checkpoint(control, work)?;
            *sum += value;
            if !sum.is_finite() {
                return Err(numeric_error());
            }
        }
    }
    for cluster in 0..CLUSTER_COUNT {
        if counts[cluster] == 0 {
            continue;
        }
        let count = f64::from(counts[cluster]);
        for (centroid, sum) in centroids[cluster].iter_mut().zip(&sums[cluster]) {
            *centroid = *sum / count;
            if !centroid.is_finite() {
                return Err(numeric_error());
            }
        }
    }
    Ok(centroids)
}

fn squared_distance(
    left: &[f64],
    right: &[f64],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<f64, AlgorithmError> {
    let mut distance = 0.0;
    for (&left, &right) in left.iter().zip(right) {
        checkpoint(control, work)?;
        let delta = left - right;
        distance += delta * delta;
        if !distance.is_finite() {
            return Err(numeric_error());
        }
    }
    Ok(distance)
}

fn canonical_labels(assignments: &[usize]) -> Result<Vec<i64>, AlgorithmError> {
    let mut canonical = [usize::MAX; CLUSTER_COUNT];
    let mut next = 0;
    assignments
        .iter()
        .map(|&cluster| {
            if canonical[cluster] == usize::MAX {
                canonical[cluster] = next;
                next += 1;
            }
            i64::try_from(canonical[cluster])
                .map_err(|_| execution("K-means community ID exceeds Int64"))
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

fn run_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("K-means assignment worker panicked")),
    }
}

fn first_chunk_error<T>(results: Vec<Result<T, AlgorithmError>>) -> Result<Vec<T>, AlgorithmError> {
    results.into_iter().collect()
}

/// Choose serial vs private-pool parallel assignment for a K-means workload.
pub(crate) fn select_kmeans_assign_path(
    control: &AlgorithmControl,
    points: usize,
    dimension: usize,
) -> KMeansAssignPath {
    let threads = control.compute_threads();
    let estimated_ops = estimated_assign_ops(points, dimension);
    if threads <= 1 || points <= 1 || estimated_ops < KMEANS_ASSIGN_PARALLEL_CROSSOVER_OPS {
        return KMeansAssignPath::Serial;
    }
    if control
        .compute_pool()
        .is_none_or(|pool| !pool.is_parallel())
    {
        return KMeansAssignPath::Serial;
    }
    let chunks = point_chunks(points, threads).len();
    if chunks <= 1 {
        return KMeansAssignPath::Serial;
    }
    KMeansAssignPath::Parallel { threads, chunks }
}

fn estimated_assign_ops(points: usize, dimension: usize) -> u64 {
    let points = u64::try_from(points).unwrap_or(u64::MAX);
    let dimension = u64::try_from(dimension).unwrap_or(u64::MAX);
    points
        .saturating_mul(CLUSTER_COUNT as u64)
        .saturating_mul(dimension)
}

fn point_chunks(points: usize, threads: usize) -> Vec<(usize, usize)> {
    if points == 0 {
        return Vec::new();
    }
    let workers = threads.clamp(1, points);
    let base = points / workers;
    let rem = points % workers;
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
    execution("K-means numeric state is NaN or infinite")
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

    fn broad_fixture() -> Vec<Vec<f64>> {
        (0..300)
            .map(|point| {
                let group = point % CLUSTER_COUNT;
                let member = point / CLUSTER_COUNT;
                vec![
                    group as f64 * 1_000.0 + member as f64 * 0.01,
                    group as f64 * 10.0,
                    member as f64,
                    (group * group) as f64,
                    (point % 7) as f64,
                    (point % 11) as f64,
                    (point % 13) as f64,
                    (point % 17) as f64,
                    (point % 19) as f64,
                    (point % 23) as f64,
                    (point % 29) as f64,
                    (point % 31) as f64,
                ]
            })
            .collect()
    }

    #[test]
    fn separated_pairs_form_ten_stable_topology_ordered_groups() {
        let values = (0..20)
            .map(|point| vec![f64::from(point / 2 * 10) + f64::from(point % 2) * 0.25])
            .collect::<Vec<_>>();
        let vectors = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let expected = (0..10)
            .flat_map(|group| [group, group])
            .collect::<Vec<i64>>();
        assert_eq!(fit(&vectors, &control(), MAX_ITERATIONS).unwrap(), expected);
        assert_eq!(fit(&vectors, &control(), MAX_ITERATIONS).unwrap(), expected);
    }

    #[test]
    fn assign_path_respects_crossover_threads_and_pool() {
        assert_eq!(
            select_kmeans_assign_path(&control(), 512, 16),
            KMeansAssignPath::Serial
        );

        let parallel = control_with_threads(4);
        assert_eq!(
            select_kmeans_assign_path(&parallel, 204, 16),
            KMeansAssignPath::Serial
        );
        assert_eq!(
            select_kmeans_assign_path(&parallel, 205, 16),
            KMeansAssignPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }

    #[test]
    fn thread_matrix_preserves_kmeans_assignment_fingerprint() {
        let values = broad_fixture();
        let vectors = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let serial = stable_labels(&vectors, &control_with_threads(1)).unwrap();
        for threads in [2, 4, 8] {
            assert_eq!(
                stable_labels(&vectors, &control_with_threads(threads)).unwrap(),
                serial,
                "threads={threads}"
            );
        }
    }

    #[test]
    fn empty_small_invalid_and_numeric_boundaries_are_structured() {
        assert!(fit(&[], &control(), MAX_ITERATIONS).unwrap().is_empty());
        let small = vec![vec![0.0]; 9];
        let small = small.iter().map(Vec::as_slice).collect::<Vec<_>>();
        assert!(matches!(
            fit(&small, &control(), MAX_ITERATIONS),
            Err(AlgorithmError::Execution { .. })
        ));
        let huge = (0..10)
            .map(|point| vec![if point == 0 { f64::MAX } else { -f64::MAX }])
            .collect::<Vec<_>>();
        let huge = huge.iter().map(Vec::as_slice).collect::<Vec<_>>();
        assert_eq!(fit(&huge, &control(), MAX_ITERATIONS), Err(numeric_error()));
    }

    #[test]
    fn duplicates_keep_empty_centroids_and_ties_choose_topology_order() {
        let duplicates = vec![vec![1.0]; 10];
        let vectors = duplicates.iter().map(Vec::as_slice).collect::<Vec<_>>();
        assert_eq!(
            fit(&vectors, &control(), MAX_ITERATIONS).unwrap(),
            vec![0; 10]
        );
        let mut work = 0;
        let centroids = (0..10)
            .map(|value| vec![f64::from(value)])
            .collect::<Vec<_>>();
        let updated = update(&vectors, &[0; 10], centroids.clone(), &control(), &mut work).unwrap();
        assert_eq!(&updated[1..], &centroids[1..]);
        let assigned = assign(&[&[1.0]], &[vec![0.0], vec![2.0]], &control(), &mut work).unwrap();
        assert_eq!(assigned, vec![0]);
        let tied = [0.0, 10.0, -10.0, 1.0, -1.0, 2.0, -2.0, 3.0, -3.0, 4.0, -4.0]
            .into_iter()
            .map(|value| vec![value])
            .collect::<Vec<_>>();
        let tied = tied.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let centroids = initialize(&tied, &control(), &mut work).unwrap();
        assert_eq!(centroids[1], vec![10.0]);
    }

    #[test]
    fn cancellation_and_local_iteration_guard_are_structured() {
        let values = vec![vec![0.0]; 10];
        let vectors = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let cancelled = AlgorithmControl::new(AlgorithmLimits::default(), cancellation);
        assert_eq!(
            fit(&vectors, &cancelled, MAX_ITERATIONS).unwrap_err(),
            AlgorithmError::Cancelled
        );
        assert_eq!(
            fit(&vectors, &control(), 0).unwrap_err(),
            AlgorithmError::NonConvergence { iterations: 0 }
        );
    }
}
