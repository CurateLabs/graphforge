//! Deterministic random walks over graph-native UUID adjacency.
//!
//! Parallelism (#553) partitions independent `(source ordinal, walk ordinal)`
//! tasks through the instance-owned private compute pool. Each walk keeps the
//! exact serial RNG stream and choice ordering, and worker outputs merge by
//! canonical task ordinal so the public row order remains byte-identical.

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};

use rayon::prelude::*;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

/// Changing the generator, seed derivation, draw conversion, or choice ordering
/// requires a new contract version.
pub(crate) const RANDOM_WALK_RNG_CONTRACT: &str = "splitmix64-v1";

/// Estimated transitions below which random_walk stays on the serial path (#553).
///
/// This mirrors Node2Vec walk-corpus generation: the random-walk task body is
/// similarly independent per start/walk pair, and the same release-mode M4
/// crossover evidence showed pool scheduling/merge tax dominating below this
/// boundary while larger walk corpora benefit from private workers.
pub const RANDOM_WALK_PARALLEL_CROSSOVER: u64 = 256;

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
pub(crate) trait RandomWalkAdjacencySource: Sync {
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

    let plan = RandomWalkPlan {
        sources,
        walks_per_source,
        walk_length,
        seed,
        weighted,
        output_rows,
    };
    let transitions = u64::try_from(transitions).unwrap_or(u64::MAX);
    match select_random_walk_path(control, plan.output_rows, transitions) {
        RandomWalkExecutionPath::Serial => random_walks_serial(adjacency, plan, control),
        RandomWalkExecutionPath::Parallel { .. } => random_walks_parallel(adjacency, plan, control),
    }
}

#[derive(Clone, Copy)]
struct RandomWalkPlan<'a> {
    sources: &'a [[u8; 16]],
    walks_per_source: usize,
    walk_length: usize,
    seed: u64,
    weighted: bool,
    output_rows: usize,
}

#[derive(Clone, Copy)]
struct RandomWalkTask {
    source_uuid: [u8; 16],
    source_ordinal: usize,
    walk_ordinal: usize,
}

fn random_walks_serial<A: RandomWalkAdjacencySource>(
    adjacency: &A,
    plan: RandomWalkPlan<'_>,
    control: &AlgorithmControl,
) -> Result<Vec<Vec<[u8; 16]>>, AlgorithmError> {
    let mut output = Vec::with_capacity(plan.output_rows);
    for (source_ordinal, source_uuid) in plan.sources.iter().enumerate() {
        for walk_ordinal in 0..plan.walks_per_source {
            output.push(build_walk(
                adjacency,
                RandomWalkTask {
                    source_uuid: *source_uuid,
                    source_ordinal,
                    walk_ordinal,
                },
                plan,
                control,
            )?);
        }
    }
    Ok(output)
}

fn random_walks_parallel<A: RandomWalkAdjacencySource>(
    adjacency: &A,
    plan: RandomWalkPlan<'_>,
    control: &AlgorithmControl,
) -> Result<Vec<Vec<[u8; 16]>>, AlgorithmError> {
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel random_walk requires an instance-owned compute pool"))?;
    let ranges = walk_task_chunks(plan.output_rows, control.compute_threads());
    let chunk_results = run_on_pool(pool, || {
        Ok(ranges
            .par_iter()
            .map(|&(start, end)| {
                let mut walks = Vec::with_capacity(end.saturating_sub(start));
                for task in start..end {
                    let source_ordinal = task / plan.walks_per_source;
                    let walk_ordinal = task % plan.walks_per_source;
                    walks.push(build_walk(
                        adjacency,
                        RandomWalkTask {
                            source_uuid: plan.sources[source_ordinal],
                            source_ordinal,
                            walk_ordinal,
                        },
                        plan,
                        control,
                    )?);
                }
                Ok(walks)
            })
            .collect::<Vec<Result<Vec<Vec<[u8; 16]>>, AlgorithmError>>>())
    })?;
    merge_walk_chunks(chunk_results, plan.output_rows)
}

fn build_walk<A: RandomWalkAdjacencySource>(
    adjacency: &A,
    task: RandomWalkTask,
    plan: RandomWalkPlan<'_>,
    control: &AlgorithmControl,
) -> Result<Vec<[u8; 16]>, AlgorithmError> {
    control.check_cancelled()?;
    let mut rng = SplitMix64::new(derive_seed(
        plan.seed,
        task.source_ordinal,
        task.walk_ordinal,
    ));
    let mut node = task.source_uuid;
    let mut walk = Vec::new();
    walk.push(task.source_uuid);

    for _ in 0..plan.walk_length {
        control.checkpoint()?;
        let mut choices = adjacency.choices(&node)?;
        choices.sort_unstable_by_key(|edge| (edge.neighbor_uuid, edge.edge_uuid));
        let choices = choices.iter().collect::<Vec<_>>();
        let Some(edge) = choose(&choices, plan.weighted, &mut rng)? else {
            break;
        };
        node = edge.neighbor_uuid;
        walk.push(node);
    }
    Ok(walk)
}

fn merge_walk_chunks(
    chunks: Vec<Result<Vec<Vec<[u8; 16]>>, AlgorithmError>>,
    capacity: usize,
) -> Result<Vec<Vec<[u8; 16]>>, AlgorithmError> {
    let mut output = Vec::with_capacity(capacity);
    for chunk in chunks {
        output.extend(chunk?);
    }
    Ok(output)
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
        Err(_) => Err(execution("random-walk worker panicked")),
    }
}

/// Selected execution path for observability and crossover tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RandomWalkExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

/// Choose serial vs private-pool parallel walk generation.
pub(crate) fn select_random_walk_path(
    control: &AlgorithmControl,
    total_walks: usize,
    estimated_transitions: u64,
) -> RandomWalkExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1 || total_walks <= 1 || estimated_transitions < RANDOM_WALK_PARALLEL_CROSSOVER {
        return RandomWalkExecutionPath::Serial;
    }
    if control
        .compute_pool()
        .is_none_or(|pool| !pool.is_parallel())
    {
        return RandomWalkExecutionPath::Serial;
    }
    let chunks = walk_task_chunks(total_walks, threads).len();
    if chunks <= 1 {
        return RandomWalkExecutionPath::Serial;
    }
    RandomWalkExecutionPath::Parallel { threads, chunks }
}

fn walk_task_chunks(total_walks: usize, threads: usize) -> Vec<(usize, usize)> {
    if total_walks == 0 {
        return Vec::new();
    }
    let workers = threads.clamp(1, total_walks);
    let base = total_walks / workers;
    let rem = total_walks % workers;
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

fn execution(message: impl Into<String>) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.into(),
    }
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
    use crate::compute_pool::ComputePool;

    use super::*;
    use std::sync::Arc;

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

    fn control_with_threads(threads: usize) -> AlgorithmControl {
        let pool = Arc::new(ComputePool::new(threads).unwrap());
        AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(threads),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(pool)
    }

    fn parallel_fixture() -> (RandomWalkAdjacency, Vec<[u8; 16]>) {
        let mut graph = HashMap::new();
        for node in 0..32_u8 {
            graph.insert(
                uuid(node),
                vec![
                    edge(node.wrapping_add(64), node.wrapping_add(1) % 32, 2.0),
                    edge(node.wrapping_add(32), node.wrapping_add(3) % 32, 1.0),
                    edge(node.wrapping_add(96), node.wrapping_add(5) % 32, 3.0),
                ],
            );
        }
        let sources = (0..16_u8).map(uuid).collect::<Vec<_>>();
        (graph, sources)
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

    #[test]
    fn random_walk_path_selection_uses_private_pool_above_crossover() {
        assert_eq!(
            select_random_walk_path(&control(), 64, RANDOM_WALK_PARALLEL_CROSSOVER),
            RandomWalkExecutionPath::Serial
        );
        assert_eq!(
            select_random_walk_path(
                &control_with_threads(4),
                64,
                RANDOM_WALK_PARALLEL_CROSSOVER - 1
            ),
            RandomWalkExecutionPath::Serial
        );
        assert_eq!(
            select_random_walk_path(&control_with_threads(4), 64, RANDOM_WALK_PARALLEL_CROSSOVER),
            RandomWalkExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }

    #[test]
    fn parallel_walks_match_serial_oracle_at_supported_thread_counts() {
        let (graph, sources) = parallel_fixture();
        let serial = random_walks(&graph, &sources, 4, 8, 99, true, &control()).unwrap();
        for threads in [2, 4, 8] {
            let parallel = random_walks(
                &graph,
                &sources,
                4,
                8,
                99,
                true,
                &control_with_threads(threads),
            )
            .unwrap();
            assert_eq!(parallel, serial, "threads={threads}");
        }
    }

    #[test]
    fn parallel_cancellation_returns_structured_error_without_rows() {
        let (graph, sources) = parallel_fixture();
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let pool = Arc::new(ComputePool::new(4).unwrap());
        let control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            cancellation,
        )
        .with_compute_pool(pool);
        assert_eq!(
            random_walks(&graph, &sources, 4, 8, 99, true, &control),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn parallel_worker_panic_returns_structured_error() {
        struct PanicAdjacency;

        impl RandomWalkAdjacencySource for PanicAdjacency {
            fn choices(&self, _node: &[u8; 16]) -> Result<Vec<RandomWalkEdge>, AlgorithmError> {
                panic!("test worker panic");
            }
        }

        let sources = (0..16_u8).map(uuid).collect::<Vec<_>>();
        let error = random_walks(
            &PanicAdjacency,
            &sources,
            4,
            8,
            99,
            false,
            &control_with_threads(4),
        )
        .unwrap_err();
        assert_eq!(
            error,
            AlgorithmError::Execution {
                message: "random-walk worker panicked".into()
            }
        );
    }
}
