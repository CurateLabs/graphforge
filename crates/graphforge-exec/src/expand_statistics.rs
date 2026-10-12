//! Row and byte estimates for the traversal operators (#1985).
//!
//! `ExpandExec` and `VarLenExpandExec` read their adjacency at execution, so
//! DataFusion cannot know how many rows they emit. Left unpublished, the
//! statistics of every plan node above a traversal are unknown, and the
//! [`join_memory`](crate::join_memory) rule never sees that a join whose build
//! side is a traversal's output cannot fit the query memory pool.
//!
//! The estimate is the input row count times the degree of the relation in the
//! traversed direction, from the CSR manifest the session already holds. The
//! degree is that of the densest shard: a mean over every node of the graph
//! would be near one for a relation whose sources are a small, dense block of
//! the node identity space (a person's messages), and the estimate exists to
//! stop a large join from being planned as a hash join, so it errs high. A
//! relation without a fresh persisted CSR publishes nothing: reading its view
//! would scan the edge table during planning.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::Statistics;
use datafusion::common::stats::Precision;
use datafusion::physical_plan::ExecutionPlan;
use graphforge_ir::Direction;
use graphforge_plan::VarLenMode;

use crate::adjacency::{AdjacencyProvider, AdjacencyStatus};

/// Hops of an unbounded variable-length expansion the estimate follows.
const UNBOUNDED_HOPS_ESTIMATED: u32 = 8;

/// Adjacency a traversal reads, as far as planning can know it.
struct RelationDegree {
    /// Adjacency entries per source node, as the densest shard holds them.
    per_node: u64,
    /// Adjacency entries of the relation in the traversed direction.
    entries: u64,
    /// Exclusive extent of the node identities the relation's view covers: no
    /// search reaches more distinct nodes than this.
    nodes: u64,
}

/// The degree of `relation` in `direction`, or `None` unless the session serves
/// it from a fresh persisted index. Planning never fails on a missing estimate.
fn relation_degree(
    provider: &dyn AdjacencyProvider,
    relation: &str,
    direction: Direction,
) -> Option<RelationDegree> {
    if provider.status(relation, direction) != AdjacencyStatus::Hit {
        return None;
    }
    let view = provider.adjacency(relation, direction).ok()?;
    Some(RelationDegree {
        per_node: view.planning_degree().max(1),
        entries: view.edge_entry_count().ok()?,
        nodes: view.node_extent(),
    })
}

/// Statistics of an operator over `schema` emitting `rows`.
///
/// Only the row count is published. DataFusion's join selection compares byte
/// sizes against its collect threshold when both sides publish one, and the
/// width of a traversal's output schema is an upper bound on what a row holds
/// (every variable-width column is charged a fixed size), so a byte size would
/// push a traversal's output over a threshold its rows are under and reorder
/// joins the estimate has no business reordering. The join memory rule derives
/// the bytes it compares with the pool from the rows and the schema's row width.
fn statistics(schema: &SchemaRef, rows: Precision<usize>) -> Arc<Statistics> {
    let mut statistics = Statistics::new_unknown(schema);
    statistics.num_rows = rows;
    Arc::new(statistics)
}

/// Which part of a traversal's output a statistics request is about.
#[derive(Clone, Copy)]
pub(crate) struct Scope {
    /// The requested output partition; `None` is the whole plan.
    pub(crate) partition: Option<usize>,
    /// Partitions the traversal's output has.
    pub(crate) output_partitions: usize,
}

impl Scope {
    /// The statistics of the requested part, given those of the whole plan.
    ///
    /// The whole plan is estimated first, from the input's whole-plan
    /// statistics. A partition is then its even share of that: the row and byte
    /// counts divided by the output partition count, rounded up, as an inexact
    /// figure. The shares sum to at least the whole, so a plan that splits its
    /// output across partitions never publishes less than it is estimated to
    /// emit. An unknown count stays unknown. The whole plan is returned
    /// unchanged.
    ///
    /// Only counts are divided: `whole` carries no column statistics, which an
    /// even share could not describe.
    fn of(self, whole: Arc<Statistics>) -> Arc<Statistics> {
        if self.partition.is_none() {
            return whole;
        }
        let partitions = self.output_partitions.max(1);
        let mut share = Arc::unwrap_or_clone(whole);
        share.num_rows = divided(share.num_rows, partitions);
        share.total_byte_size = divided(share.total_byte_size, partitions);
        Arc::new(share)
    }
}

/// One of `partitions` even shares of `count`, rounded up.
fn divided(count: Precision<usize>, partitions: usize) -> Precision<usize> {
    match count {
        Precision::Exact(count) | Precision::Inexact(count) => {
            Precision::Inexact(count.div_ceil(partitions))
        }
        Precision::Absent => Precision::Absent,
    }
}

/// Rows a traversal emits for `input` rows, each producing `per_row` rows.
fn scaled(input: Precision<usize>, per_row: u64) -> Precision<usize> {
    match input {
        Precision::Exact(0) => Precision::Exact(0),
        Precision::Absent => Precision::Absent,
        Precision::Exact(rows) | Precision::Inexact(rows) => {
            Precision::Inexact(rows.saturating_mul(usize::try_from(per_row).unwrap_or(usize::MAX)))
        }
    }
}

/// `rows` limited to `fetch`.
fn capped(rows: Precision<usize>, fetch: Option<usize>) -> Precision<usize> {
    match (rows, fetch) {
        (Precision::Exact(rows), Some(fetch)) => Precision::Exact(rows.min(fetch)),
        (Precision::Inexact(rows), Some(fetch)) => Precision::Inexact(rows.min(fetch)),
        (Precision::Absent, Some(fetch)) => Precision::Inexact(fetch),
        (rows, None) => rows,
    }
}

/// Statistics of a one-hop expansion of `input` over `relation`.
pub(crate) fn single_hop(
    input: &Arc<dyn ExecutionPlan>,
    scope: Scope,
    provider: &dyn AdjacencyProvider,
    relation: &str,
    direction: Direction,
    fetch: Option<usize>,
    schema: &SchemaRef,
) -> datafusion::common::Result<Arc<Statistics>> {
    let input_rows = input.partition_statistics(None)?.num_rows;
    let rows = match relation_degree(provider, relation, direction) {
        Some(degree) => scaled(input_rows, degree.per_node),
        None => Precision::Absent,
    };
    Ok(scope.of(statistics(schema, capped(rows, fetch))))
}

/// What a variable-length expansion walks and emits, as planning knows it.
#[derive(Clone, Copy)]
pub(crate) struct VarLenShape {
    /// Minimum and maximum hop counts; `None` is unbounded.
    pub(crate) hops: (u16, Option<u16>),
    /// Whether the destination is an input column the reached node must equal.
    pub(crate) destination_bound: bool,
    /// Whether rows are trails or reached nodes.
    pub(crate) expansion: VarLenMode,
}

/// Statistics of a variable-length expansion of `input` over `relation`.
///
/// A seed reaches at most the sum of `degree^hops` trails for the hop counts it
/// walks. The figure is capped at the relation's adjacency entry count, which
/// keeps it finite for dense relations and unbounded patterns. A search that
/// emits one row per reached node ([`VarLenMode::Reachability`]) is capped
/// further at the node identities the relation covers: a seed reaches each node
/// at most once, however many trails lead to it. A traversal bound to a
/// destination emits only the paths that reach it, estimated as one per input
/// row.
pub(crate) fn variable_length(
    input: &Arc<dyn ExecutionPlan>,
    scope: Scope,
    provider: &dyn AdjacencyProvider,
    relation: &str,
    direction: Direction,
    shape: VarLenShape,
    schema: &SchemaRef,
) -> datafusion::common::Result<Arc<Statistics>> {
    let input_rows = input.partition_statistics(None)?.num_rows;
    let rows = match relation_degree(provider, relation, direction) {
        Some(_) if shape.destination_bound => scaled(input_rows, 1),
        Some(degree) => {
            let (min_hops, max_hops) = shape.hops;
            let first = u32::from(min_hops);
            let last = max_hops
                .map_or(UNBOUNDED_HOPS_ESTIMATED, u32::from)
                .max(first);
            let mut trails = 0_u64;
            let mut at_hop = degree.per_node.saturating_pow(first);
            for hop in first..=last {
                trails = trails.saturating_add(at_hop);
                if hop < last {
                    at_hop = at_hop.saturating_mul(degree.per_node);
                }
            }
            let mut per_seed = trails.min(degree.entries.max(1));
            if shape.expansion == VarLenMode::Reachability {
                per_seed = per_seed.min(degree.nodes.max(1));
            }
            scaled(input_rows, per_seed)
        }
        None => Precision::Absent,
    };
    Ok(scope.of(statistics(schema, rows)))
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::{DataType, Field, Schema};

    /// The statistics of a whole plan with the given row and byte counts.
    fn whole(rows: Precision<usize>, bytes: Precision<usize>) -> Arc<Statistics> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "node_id",
            DataType::UInt64,
            false,
        )]));
        let mut statistics = Statistics::new_unknown(&schema);
        statistics.num_rows = rows;
        statistics.total_byte_size = bytes;
        Arc::new(statistics)
    }

    fn scope(partition: Option<usize>, output_partitions: usize) -> Scope {
        Scope {
            partition,
            output_partitions,
        }
    }

    fn count(precision: Precision<usize>) -> usize {
        match precision {
            Precision::Exact(count) | Precision::Inexact(count) => count,
            Precision::Absent => panic!("a known count"),
        }
    }

    #[test]
    fn the_whole_plan_is_returned_unchanged() {
        let plan = whole(Precision::Exact(10), Precision::Inexact(100));
        for partitions in [1, 3] {
            let kept = scope(None, partitions).of(plan.clone());
            assert_eq!(kept.num_rows, Precision::Exact(10));
            assert_eq!(kept.total_byte_size, Precision::Inexact(100));
        }
    }

    #[test]
    fn a_partition_is_its_even_share_rounded_up_and_inexact() {
        let plan = whole(Precision::Exact(10), Precision::Inexact(100));
        for partition in 0..3 {
            let share = scope(Some(partition), 3).of(plan.clone());
            assert_eq!(share.num_rows, Precision::Inexact(4));
            assert_eq!(share.total_byte_size, Precision::Inexact(34));
        }
    }

    #[test]
    fn unknown_counts_stay_unknown() {
        let plan = whole(Precision::Absent, Precision::Exact(9));
        let share = scope(Some(1), 3).of(plan);
        assert_eq!(share.num_rows, Precision::Absent);
        assert_eq!(share.total_byte_size, Precision::Inexact(3));
    }

    #[test]
    fn the_only_output_partition_is_the_whole_estimate() {
        let plan = whole(Precision::Inexact(7), Precision::Absent);
        let share = scope(Some(0), 1).of(plan);
        assert_eq!(share.num_rows, Precision::Inexact(7));
        assert_eq!(share.total_byte_size, Precision::Absent);
    }

    #[test]
    fn the_partition_estimates_sum_to_at_least_the_whole_estimate() {
        for partitions in 2..=9 {
            for rows in [0, 1, 2, 7, 10, 64, 1001] {
                let bytes = rows * 13 + 1;
                let plan = whole(Precision::Inexact(rows), Precision::Inexact(bytes));
                let (rows_sum, bytes_sum) = (0..partitions)
                    .map(|partition| scope(Some(partition), partitions).of(plan.clone()))
                    .fold((0, 0), |(rows_sum, bytes_sum), share| {
                        (
                            rows_sum + count(share.num_rows),
                            bytes_sum + count(share.total_byte_size),
                        )
                    });
                assert!(
                    rows_sum >= rows,
                    "{partitions} partitions of {rows} rows sum to {rows_sum}"
                );
                assert!(
                    bytes_sum >= bytes,
                    "{partitions} partitions of {bytes} bytes sum to {bytes_sum}"
                );
            }
        }
    }
}
