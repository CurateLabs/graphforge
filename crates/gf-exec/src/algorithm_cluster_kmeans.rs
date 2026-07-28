//! Deterministic, dependency-free K-means kernel for vector clustering.
use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

pub(crate) const CLUSTER_COUNT: usize = 10;
const MAX_ITERATIONS: usize = 100;
const CHECKPOINT_INTERVAL: usize = 64;

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

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
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
