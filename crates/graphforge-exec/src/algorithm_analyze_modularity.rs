//! Exact weighted modularity for an explicit graph-layer partition.

use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind};

use graphforge_core::algorithms::{Algorithm, AnalyzeAlgorithm};
use rayon::prelude::*;

use crate::algorithm_dispatch::{
    AlgorithmControl, AlgorithmError, AlgorithmLimits, AlgorithmOutput, AlgorithmValue,
};
use crate::algorithm_partition::ResolvedPartitionMap;

/// Community counts below this stay serial to avoid private-pool scheduling tax.
pub(crate) const MODULARITY_PARALLEL_CROSSOVER_COMMUNITIES: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct ModularityEdge {
    pub edge_uuid: [u8; 16],
    pub source_uuid: [u8; 16],
    pub target_uuid: [u8; 16],
    pub weight: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ModularityExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

/// Compute classic weighted undirected modularity at fixed resolution 1.0.
#[allow(
    clippy::too_many_lines,
    reason = "serial and parallel modularity paths share one validated entrypoint"
)]
pub(crate) fn modularity(
    nodes: &[[u8; 16]],
    edges: &[ModularityEdge],
    directed: bool,
    partitions: &ResolvedPartitionMap,
    control: &AlgorithmControl,
) -> Result<f64, AlgorithmError> {
    let adjacency_entries = u64::try_from(edges.len())
        .unwrap_or(u64::MAX)
        .saturating_mul(2);
    control.check_graph_size(nodes.len(), adjacency_entries)?;
    control.check_output_rows(1)?;
    control.checkpoint()?;
    if directed {
        return Err(execution("modularity requires an undirected graph"));
    }

    let selected = nodes.iter().copied().collect::<BTreeSet<_>>();
    if selected.len() != nodes.len() {
        return Err(execution("modularity node UUIDs must be unique"));
    }
    if !partitions.iter().map(|(node, _)| node).eq(selected.iter()) {
        return Err(execution("partition mapping must exactly match selection"));
    }
    if selected.is_empty() {
        return Err(undefined());
    }

    let mut stored = BTreeMap::new();
    let mut work = 0;
    for &raw in edges {
        checkpoint(control, &mut work)?;
        if !raw.weight.is_finite() || raw.weight < 0.0 {
            return Err(execution(
                "modularity weights must be finite and nonnegative",
            ));
        }
        let edge = canonical(raw);
        if !selected.contains(&edge.source_uuid) || !selected.contains(&edge.target_uuid) {
            return Err(execution("modularity edge endpoint is outside selection"));
        }
        if let Some(previous) = stored.insert(edge.edge_uuid, edge)
            && previous != edge
        {
            return Err(execution("modularity edge UUID is inconsistent"));
        }
    }

    let community_order = communities_by_minimum_uuid(partitions);
    let mut degrees = selected
        .iter()
        .map(|node| (*node, 0.0))
        .collect::<BTreeMap<_, _>>();
    let mut internal_weights = community_order
        .iter()
        .map(|partition| (partition.clone(), 0.0))
        .collect::<BTreeMap<_, _>>();
    let mut total_weight = 0.0;
    for edge in stored.into_values() {
        checkpoint(control, &mut work)?;
        total_weight = finite(total_weight + edge.weight)?;
        if edge.source_uuid == edge.target_uuid {
            add(&mut degrees, edge.source_uuid, finite(2.0 * edge.weight)?)?;
        } else {
            add(&mut degrees, edge.source_uuid, edge.weight)?;
            add(&mut degrees, edge.target_uuid, edge.weight)?;
        }
        let source_partition = partitions
            .get(&edge.source_uuid)
            .expect("validated partition mapping")
            .as_str();
        let target_partition = partitions
            .get(&edge.target_uuid)
            .expect("validated partition mapping")
            .as_str();
        if source_partition == target_partition {
            add_string(&mut internal_weights, source_partition, edge.weight)?;
        }
    }
    if total_weight == 0.0 {
        return Err(undefined());
    }
    let two_m = finite(2.0 * total_weight)?;

    let mut volumes = internal_weights
        .keys()
        .map(|partition| (partition.clone(), 0.0))
        .collect::<BTreeMap<_, _>>();
    for (node, degree) in degrees {
        checkpoint(control, &mut work)?;
        let partition = partitions
            .get(&node)
            .expect("validated partition mapping")
            .as_str();
        add_string(&mut volumes, partition, degree)?;
    }

    let contributions = match select_modularity_path(control, community_order.len()) {
        ModularityExecutionPath::Serial => modularity_contributions_serial(
            &community_order,
            &internal_weights,
            &volumes,
            total_weight,
            two_m,
            control,
            &mut work,
        )?,
        ModularityExecutionPath::Parallel { .. } => modularity_contributions_parallel(
            &community_order,
            &internal_weights,
            &volumes,
            total_weight,
            two_m,
            control,
        )?,
    };
    let mut score = 0.0;
    for contribution in contributions {
        score = finite(score + contribution)?;
    }
    control.check_cancelled()?;
    finite(score)
}

pub(crate) fn select_modularity_path(
    control: &AlgorithmControl,
    communities: usize,
) -> ModularityExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || communities < MODULARITY_PARALLEL_CROSSOVER_COMMUNITIES
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return ModularityExecutionPath::Serial;
    }
    ModularityExecutionPath::Parallel {
        threads,
        chunks: community_chunks(communities, threads).len(),
    }
}

/// Shape the stable scalar result owned by the modularity contract.
pub(crate) fn modularity_output(value: f64) -> Result<AlgorithmOutput, AlgorithmError> {
    let value = finite(value)?;
    crate::algorithm_output::shape_logical_rows(
        Algorithm::Analyze(AnalyzeAlgorithm::Modularity),
        [vec![AlgorithmValue::Float64(value)]],
        AlgorithmLimits::default().batch_size,
        AlgorithmLimits::default().output_rows,
    )
}

fn canonical(mut edge: ModularityEdge) -> ModularityEdge {
    if edge.target_uuid < edge.source_uuid {
        std::mem::swap(&mut edge.source_uuid, &mut edge.target_uuid);
    }
    edge
}

fn communities_by_minimum_uuid(partitions: &ResolvedPartitionMap) -> Vec<String> {
    let mut seen = BTreeSet::new();
    partitions
        .iter()
        .filter_map(|(_, partition)| {
            let partition = partition.as_str().to_owned();
            seen.insert(partition.clone()).then_some(partition)
        })
        .collect()
}

fn modularity_contributions_serial(
    community_order: &[String],
    internal_weights: &BTreeMap<String, f64>,
    volumes: &BTreeMap<String, f64>,
    total_weight: f64,
    two_m: f64,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<f64>, AlgorithmError> {
    let mut contributions = Vec::with_capacity(community_order.len());
    for partition in community_order {
        checkpoint(control, work)?;
        contributions.push(modularity_contribution(
            partition,
            internal_weights,
            volumes,
            total_weight,
            two_m,
        )?);
    }
    Ok(contributions)
}

fn modularity_contributions_parallel(
    community_order: &[String],
    internal_weights: &BTreeMap<String, f64>,
    volumes: &BTreeMap<String, f64>,
    total_weight: f64,
    two_m: f64,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel modularity requires an instance-owned compute pool"))?;
    let ranges = community_chunks(community_order.len(), control.compute_threads());
    let mut chunk_results = run_on_pool(pool, || {
        Ok(ranges
            .par_iter()
            .map(|&(start, end)| {
                let result = (|| {
                    control.check_cancelled()?;
                    let mut work = 0_usize;
                    let mut local = Vec::with_capacity(end - start);
                    for partition in &community_order[start..end] {
                        checkpoint(control, &mut work)?;
                        local.push(modularity_contribution(
                            partition,
                            internal_weights,
                            volumes,
                            total_weight,
                            two_m,
                        )?);
                    }
                    Ok(local)
                })();
                (start, result)
            })
            .collect::<Vec<(usize, Result<Vec<f64>, AlgorithmError>)>>())
    })?;
    chunk_results.sort_unstable_by_key(|(start, _)| *start);
    let mut contributions = Vec::with_capacity(community_order.len());
    for (_, chunk) in chunk_results {
        contributions.extend(chunk?);
    }
    Ok(contributions)
}

fn modularity_contribution(
    partition: &str,
    internal_weights: &BTreeMap<String, f64>,
    volumes: &BTreeMap<String, f64>,
    total_weight: f64,
    two_m: f64,
) -> Result<f64, AlgorithmError> {
    let internal = internal_weights[partition];
    let volume_ratio = volumes[partition] / two_m;
    finite(internal / total_weight - volume_ratio * volume_ratio)
}

fn community_chunks(len: usize, threads: usize) -> Vec<(usize, usize)> {
    if len == 0 {
        return Vec::new();
    }
    let workers = threads.clamp(1, len);
    let base = len / workers;
    let rem = len % workers;
    let mut chunks = Vec::with_capacity(workers);
    let mut start = 0;
    for index in 0..workers {
        let chunk_len = base + usize::from(index < rem);
        let end = start + chunk_len;
        if start < end {
            chunks.push((start, end));
        }
        start = end;
    }
    chunks
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
        Err(_) => Err(execution("modularity worker panicked")),
    }
}

fn add(
    values: &mut BTreeMap<[u8; 16], f64>,
    key: [u8; 16],
    value: f64,
) -> Result<(), AlgorithmError> {
    let current = values.get_mut(&key).expect("validated node");
    *current = finite(*current + value)?;
    Ok(())
}

fn add_string(
    values: &mut BTreeMap<String, f64>,
    key: &str,
    value: f64,
) -> Result<(), AlgorithmError> {
    let current = values.get_mut(key).expect("validated partition");
    *current = finite(*current + value)?;
    Ok(())
}

fn finite(value: f64) -> Result<f64, AlgorithmError> {
    value
        .is_finite()
        .then_some(value)
        .ok_or_else(|| execution("modularity accumulation exceeds finite range"))
}

fn checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    *work += 1;
    if work.is_multiple_of(4_096) {
        control.checkpoint()?;
    } else {
        control.check_cancelled()?;
    }
    Ok(())
}

fn undefined() -> AlgorithmError {
    AlgorithmError::UndefinedModularity
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
    use crate::algorithm_output::shape_algorithm_output;
    use crate::algorithm_partition::PartitionValue;
    use crate::compute_pool::ComputePool;
    use arrow::array::Float64Array;
    use arrow::datatypes::DataType;
    use std::sync::Arc;

    fn uuid(value: u8) -> [u8; 16] {
        [value; 16]
    }

    fn wide_uuid(value: u128) -> [u8; 16] {
        value.to_be_bytes()
    }

    fn edge(id: u8, source: u8, target: u8, weight: f64) -> ModularityEdge {
        ModularityEdge {
            edge_uuid: uuid(id),
            source_uuid: uuid(source),
            target_uuid: uuid(target),
            weight,
        }
    }

    fn wide_edge(id: u128, source: usize, target: usize, weight: f64) -> ModularityEdge {
        ModularityEdge {
            edge_uuid: wide_uuid(id),
            source_uuid: wide_uuid(source as u128),
            target_uuid: wide_uuid(target as u128),
            weight,
        }
    }

    fn mapping(values: &[(u8, PartitionValue)]) -> ResolvedPartitionMap {
        ResolvedPartitionMap::try_new(
            values.iter().map(|(node, _)| uuid(*node)),
            values
                .iter()
                .map(|(node, value)| (uuid(*node), value.clone())),
        )
        .unwrap()
    }

    fn strings(values: &[(u8, &str)]) -> ResolvedPartitionMap {
        mapping(
            &values
                .iter()
                .map(|(node, value)| (*node, PartitionValue::String((*value).into())))
                .collect::<Vec<_>>(),
        )
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn control_with_threads(threads: usize) -> AlgorithmControl {
        AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(threads),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(ComputePool::new(threads).unwrap()))
    }

    fn singleton_fixture(
        nodes: usize,
    ) -> (Vec<[u8; 16]>, Vec<ModularityEdge>, ResolvedPartitionMap) {
        let node_ids = (0..nodes)
            .map(|node| wide_uuid(node as u128))
            .collect::<Vec<_>>();
        let edges = (0..nodes - 1)
            .map(|source| {
                wide_edge(
                    10_000 + source as u128,
                    source,
                    source + 1,
                    1.0 + (source % 7) as f64,
                )
            })
            .collect::<Vec<_>>();
        let partitions = ResolvedPartitionMap::try_new(
            node_ids.iter().copied(),
            (0..nodes).map(|node| {
                (
                    wide_uuid(node as u128),
                    PartitionValue::String(format!("p{node:04}")),
                )
            }),
        )
        .unwrap();
        (node_ids, edges, partitions)
    }

    fn run(
        nodes: &[[u8; 16]],
        edges: &[ModularityEdge],
        partitions: &ResolvedPartitionMap,
    ) -> Result<f64, AlgorithmError> {
        modularity(nodes, edges, false, partitions, &control())
    }

    fn close(actual: f64, expected: f64) {
        assert!((actual - expected).abs() <= 1e-12, "{actual} != {expected}");
    }

    #[test]
    fn exact_partition_all_one_and_singletons() {
        let nodes = [uuid(1), uuid(2), uuid(3), uuid(4)];
        let edges = [edge(1, 1, 2, 2.0), edge(2, 2, 3, 1.0), edge(3, 3, 4, 2.0)];
        close(
            run(
                &nodes,
                &edges,
                &strings(&[(1, "a"), (2, "a"), (3, "b"), (4, "b")]),
            )
            .unwrap(),
            0.3,
        );
        close(
            run(
                &nodes,
                &edges,
                &strings(&[(1, "a"), (2, "a"), (3, "a"), (4, "a")]),
            )
            .unwrap(),
            0.0,
        );
        close(
            run(
                &nodes,
                &edges,
                &strings(&[(1, "1"), (2, "2"), (3, "3"), (4, "4")]),
            )
            .unwrap(),
            -0.26,
        );
    }

    #[test]
    fn weighted_loops_parallel_edges_and_mapping_normalization_are_exact() {
        let nodes = [uuid(1), uuid(2), uuid(3)];
        let edges = [
            edge(1, 1, 1, 2.0),
            edge(2, 1, 2, 1.0),
            edge(3, 2, 1, 3.0),
            edge(4, 2, 3, 4.0),
        ];
        let text = strings(&[(1, "1"), (2, "1"), (3, "2")]);
        let integers = mapping(&[
            (1, PartitionValue::Integer(1)),
            (2, PartitionValue::Integer(1)),
            (3, PartitionValue::Integer(2)),
        ]);
        close(run(&nodes, &edges, &text).unwrap(), -0.08);
        assert_eq!(run(&nodes, &edges, &text), run(&nodes, &edges, &integers));

        let mut permuted = edges;
        permuted.reverse();
        assert_eq!(
            run(&[uuid(3), uuid(1), uuid(2)], &permuted, &text),
            run(&nodes, &edges, &text)
        );

        let renamed = strings(&[(3, "first"), (2, "last"), (1, "last")]);
        assert_eq!(
            run(&nodes, &edges, &text).unwrap().to_bits(),
            run(&nodes, &edges, &renamed).unwrap().to_bits()
        );

        let three_communities = strings(&[(4, "alpha"), (2, "middle"), (3, "alpha"), (1, "zulu")]);
        assert_eq!(
            communities_by_minimum_uuid(&three_communities),
            ["zulu", "middle", "alpha"]
        );
        let singleton_nodes = [uuid(1), uuid(2), uuid(3)];
        let disparate_loops = [
            edge(7, 1, 1, 1.0),
            edge(5, 2, 2, 3.0),
            edge(6, 3, 3, 1.0e16),
        ];
        let labels = strings(&[(1, "zulu"), (2, "middle"), (3, "alpha")]);
        let relabeled = strings(&[(3, "zulu"), (1, "alpha"), (2, "middle")]);
        assert_eq!(
            run(&singleton_nodes, &disparate_loops, &labels)
                .unwrap()
                .to_bits(),
            run(&singleton_nodes, &disparate_loops, &relabeled)
                .unwrap()
                .to_bits()
        );
    }

    #[test]
    fn scalar_arrow_shape_is_stable_non_null_and_versioned() {
        let algorithm = Algorithm::Analyze(AnalyzeAlgorithm::Modularity);
        let output = modularity_output(0.25).unwrap();
        assert_eq!(output.schema, algorithm.result_schema());
        assert_eq!(output.rows(), [vec![AlgorithmValue::Float64(0.25)]]);
        let batch = shape_algorithm_output(algorithm, &output).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "modularity");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Float64);
        assert!(!batch.schema().field(0).is_nullable());
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            0.25
        );
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "modularity"
        );
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm_schema_version"],
            "1"
        );
        assert!(modularity_output(f64::NAN).is_err());
    }

    #[test]
    fn path_selection_respects_crossover_and_private_pool() {
        let serial = control_with_threads(1);
        assert_eq!(
            select_modularity_path(&serial, MODULARITY_PARALLEL_CROSSOVER_COMMUNITIES),
            ModularityExecutionPath::Serial
        );
        let below = control_with_threads(4);
        assert_eq!(
            select_modularity_path(&below, MODULARITY_PARALLEL_CROSSOVER_COMMUNITIES - 1),
            ModularityExecutionPath::Serial
        );
        let no_pool = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            select_modularity_path(&no_pool, MODULARITY_PARALLEL_CROSSOVER_COMMUNITIES),
            ModularityExecutionPath::Serial
        );
        assert_eq!(
            select_modularity_path(
                &control_with_threads(4),
                MODULARITY_PARALLEL_CROSSOVER_COMMUNITIES
            ),
            ModularityExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }

    #[test]
    fn thread_matrix_preserves_modularity_score_bits() {
        let (nodes, edges, partitions) = singleton_fixture(192);
        let serial =
            modularity(&nodes, &edges, false, &partitions, &control_with_threads(1)).unwrap();
        for threads in [2_usize, 4, 8] {
            let control = control_with_threads(threads);
            assert!(matches!(
                select_modularity_path(&control, 192),
                ModularityExecutionPath::Parallel { .. }
            ));
            assert_eq!(
                modularity(&nodes, &edges, false, &partitions, &control)
                    .unwrap()
                    .to_bits(),
                serial.to_bits()
            );
        }
    }

    #[test]
    fn invalid_inputs_and_zero_volume_are_atomic() {
        let nodes = [uuid(1), uuid(2)];
        let map = strings(&[(1, "a"), (2, "b")]);
        for result in [
            modularity(&nodes, &[edge(1, 1, 2, 1.0)], true, &map, &control()),
            run(&nodes, &[edge(1, 1, 2, f64::NAN)], &map),
            run(&nodes, &[edge(1, 1, 2, f64::INFINITY)], &map),
            run(&nodes, &[edge(1, 1, 2, -1.0)], &map),
            run(&nodes, &[edge(1, 1, 3, 1.0)], &map),
            run(&nodes, &[edge(1, 1, 2, 1.0), edge(1, 1, 2, 2.0)], &map),
            run(&[uuid(1), uuid(1)], &[], &strings(&[(1, "a")])),
        ] {
            assert!(matches!(result, Err(AlgorithmError::Execution { .. })));
        }
        assert_eq!(
            run(&[], &[], &ResolvedPartitionMap::try_new([], []).unwrap()),
            Err(AlgorithmError::UndefinedModularity)
        );
        assert_eq!(
            run(&nodes, &[], &map),
            Err(AlgorithmError::UndefinedModularity)
        );
        assert_eq!(
            run(&nodes, &[edge(1, 1, 2, 0.0)], &map),
            Err(AlgorithmError::UndefinedModularity)
        );
        let incomplete =
            ResolvedPartitionMap::try_new(nodes, [(uuid(1), PartitionValue::String("a".into()))]);
        assert!(incomplete.is_err());
        let outside = ResolvedPartitionMap::try_new(
            nodes,
            [
                (uuid(1), PartitionValue::String("a".into())),
                (uuid(3), PartitionValue::String("b".into())),
            ],
        );
        assert!(outside.is_err());
    }

    #[test]
    fn cancellation_limits_and_overflow_are_structured() {
        let nodes = [uuid(1), uuid(2)];
        let edges = [edge(1, 1, 2, 1.0)];
        let map = strings(&[(1, "a"), (2, "b")]);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            modularity(
                &nodes,
                &edges,
                false,
                &map,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
        for (limits, expected) in [
            ((1, 10, 10, 10), "node"),
            ((10, 1, 10, 10), "edge"),
            ((10, 10, 0, 10), "output"),
            ((10, 10, 10, 0), "iteration"),
        ] {
            let result = modularity(
                &nodes,
                &edges,
                false,
                &map,
                &AlgorithmControl::new(
                    AlgorithmLimits {
                        nodes: limits.0,
                        edges: limits.1,
                        output_rows: limits.2,
                        iterations: limits.3,
                        states: AlgorithmLimits::default().states,
                        batch_size: AlgorithmLimits::default().batch_size,
                        compute_threads: AlgorithmLimits::default().compute_threads,
                    },
                    AlgorithmCancellation::default(),
                ),
            );
            assert!(format!("{result:?}").to_lowercase().contains(expected));
        }
        assert!(
            run(
                &nodes,
                &[edge(1, 1, 2, f64::MAX), edge(2, 1, 2, f64::MAX)],
                &map
            )
            .is_err()
        );
    }

    #[test]
    fn bounded_small_multigraphs_match_direct_formula() {
        let nodes = [uuid(1), uuid(2), uuid(3)];
        let map = strings(&[(1, "a"), (2, "a"), (3, "b")]);
        for first in 0..=2 {
            for second in 0..=2 {
                for loop_weight in 0..=2 {
                    let edges = [
                        edge(1, 1, 2, f64::from(first)),
                        edge(2, 2, 3, f64::from(second)),
                        edge(3, 1, 1, f64::from(loop_weight)),
                    ];
                    let total = f64::from(first + second + loop_weight);
                    if total == 0.0 {
                        assert!(run(&nodes, &edges, &map).is_err());
                        continue;
                    }
                    let degrees = [
                        f64::from(first + 2 * loop_weight),
                        f64::from(first + second),
                        f64::from(second),
                    ];
                    let adjacency = [
                        [2.0 * f64::from(loop_weight), f64::from(first), 0.0],
                        [f64::from(first), 0.0, f64::from(second)],
                        [0.0, f64::from(second), 0.0],
                    ];
                    let mut direct = 0.0;
                    for i in 0..3 {
                        for j in 0..3 {
                            if (i < 2) == (j < 2) {
                                direct += adjacency[i][j] - degrees[i] * degrees[j] / (2.0 * total);
                            }
                        }
                    }
                    direct /= 2.0 * total;
                    close(run(&nodes, &edges, &map).unwrap(), direct);
                }
            }
        }
    }
}
