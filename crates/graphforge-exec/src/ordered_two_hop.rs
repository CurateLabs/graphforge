//! Order-aware two-hop path counting for `ORDER BY destination_uuid LIMIT K` (#966).
//!
//! When the terminal sort is a single ascending key on the destination node UUID
//! and both hops are fixed, identity-only expansions, materializing every
//! intermediate candidate before TopK is correct but scales with total path count.
//! This module replaces the expand chain with destination-order path counting:
//! iterate destinations in node-id order (equivalent to UUID order for monotonic
//! ordinal identity), count two-hop paths with optional edge-disjointness, emit
//! path multiplicity until the limit is satisfied.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, FixedSizeBinaryBuilder};
use arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::Stream;
use graphforge_ir::Direction;

use crate::ExpandExec;
use crate::adjacency::AdjacencyProvider;
use crate::demand;

/// Rewrite ordered two-hop identity-only plans to path counting when matched.
pub fn try_rewrite_ordered_two_hop(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(spec) = detect_ordered_two_hop(&plan) else {
        return Ok(plan);
    };
    let replacement = Arc::new(OrderedTwoHopPathCountExec::new(spec)) as Arc<dyn ExecutionPlan>;
    replace_peeled_projection(&plan, replacement)
}

fn replace_peeled_projection(
    plan: &Arc<dyn ExecutionPlan>,
    replacement: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(limit) = plan.downcast_ref::<GlobalLimitExec>() {
        return Arc::clone(plan)
            .with_new_children(vec![replace_peeled_projection(limit.input(), replacement)?]);
    }
    if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() {
        return Arc::clone(plan).with_new_children(vec![replace_peeled_projection(
            coalesce.input(),
            replacement,
        )?]);
    }
    if let Some(repartition) = plan.downcast_ref::<RepartitionExec>() {
        return Arc::clone(plan).with_new_children(vec![replace_peeled_projection(
            repartition.input(),
            replacement,
        )?]);
    }
    Ok(replacement)
}

struct OrderedTwoHopSpec {
    schema: SchemaRef,
    props: Arc<PlanProperties>,
    fetch: usize,
    rel_type_name: String,
    provider: Arc<dyn AdjacencyProvider>,
    ordinal_identities: Arc<crate::V4OrdinalIdentitySession>,
    require_edge_disjoint: bool,
}

fn peel_plan(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if let Some(limit) = plan.downcast_ref::<GlobalLimitExec>() {
        return peel_plan(limit.input());
    }
    if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() {
        return peel_plan(coalesce.input());
    }
    if let Some(repartition) = plan.downcast_ref::<RepartitionExec>() {
        return peel_plan(repartition.input());
    }
    Arc::clone(plan)
}

fn peel_expand_transport(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() {
        return peel_expand_transport(coalesce.input());
    }
    if let Some(repartition) = plan.downcast_ref::<RepartitionExec>() {
        return peel_expand_transport(repartition.input());
    }
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        return peel_expand_transport(projection.input());
    }
    Arc::clone(plan)
}

fn detect_ordered_two_hop(plan: &Arc<dyn ExecutionPlan>) -> Option<OrderedTwoHopSpec> {
    let plan = peel_plan(plan);
    let projection = plan.downcast_ref::<ProjectionExec>()?;
    if projection.expr().len() != 1 {
        return None;
    }
    let projected_column = projection.expr()[0].expr.downcast_ref::<Column>()?;
    if projection
        .input()
        .schema()
        .field(projected_column.index())
        .name()
        != "node_uuid"
    {
        return None;
    }
    let projection_children = projection.children();
    let sort_input = projection_children.first()?;
    let sort = if let Some(merge) = sort_input.downcast_ref::<SortPreservingMergeExec>() {
        merge.input().downcast_ref::<SortExec>()?
    } else {
        sort_input.downcast_ref::<SortExec>()?
    };
    let fetch = sort.fetch()?;
    if sort.expr().len() != 1 || sort.expr()[0].options.descending {
        return None;
    }
    let sort_column = sort.expr()[0].expr.downcast_ref::<Column>()?;
    if sort.input().schema().field(sort_column.index()).name() != "node_uuid" {
        return None;
    }
    let (expand2_plan, require_edge_disjoint) =
        if let Some(filter) = sort.children().first()?.downcast_ref::<FilterExec>() {
            let disjoint = filter
                .predicate()
                .downcast_ref::<ScalarFunctionExpr>()
                .is_some_and(|function| function.name() == "cypher_relationship_disjoint");
            if !disjoint {
                return None;
            }
            (peel_expand_transport(filter.children().first()?), true)
        } else {
            (peel_expand_transport(sort.children().first()?), false)
        };
    let expand2 = expand2_plan.downcast_ref::<ExpandExec>()?;
    let expand1 = expand2.children().first()?.downcast_ref::<ExpandExec>()?;
    if !expand1.is_intermediate_topology_only() || !expand2.is_destination_identity_only() {
        return None;
    }
    if expand1.rel_type_name() != expand2.rel_type_name()
        || expand1.direction() != expand2.direction()
        || expand2.direction() != Direction::Out
    {
        return None;
    }
    let ordinal_identities = expand2.ordinal_identities()?;
    if !crate::edge_count::has_complete_frontier(expand1)
        || !ordinal_identities.uuid_order_matches_ordinals()
    {
        return None;
    }
    Some(OrderedTwoHopSpec {
        schema: plan.schema(),
        props: Arc::new(
            plan.properties()
                .as_ref()
                .clone()
                .with_partitioning(Partitioning::UnknownPartitioning(1)),
        ),
        fetch,
        rel_type_name: expand2.rel_type_name().to_owned(),
        provider: Arc::clone(expand2.provider()),
        ordinal_identities,
        require_edge_disjoint,
    })
}

pub struct OrderedTwoHopPathCountExec {
    schema: SchemaRef,
    props: Arc<PlanProperties>,
    fetch: usize,
    rel_type_name: String,
    provider: Arc<dyn AdjacencyProvider>,
    ordinal_identities: Arc<crate::V4OrdinalIdentitySession>,
    require_edge_disjoint: bool,
    capture_epoch: u64,
}

impl OrderedTwoHopPathCountExec {
    fn new(spec: OrderedTwoHopSpec) -> Self {
        Self {
            schema: spec.schema,
            props: spec.props,
            fetch: spec.fetch,
            rel_type_name: spec.rel_type_name,
            provider: spec.provider,
            ordinal_identities: spec.ordinal_identities,
            require_edge_disjoint: spec.require_edge_disjoint,
            capture_epoch: demand::stamp_capture_epoch().unwrap_or(0),
        }
    }
}

impl fmt::Debug for OrderedTwoHopPathCountExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderedTwoHopPathCountExec")
            .field("fetch", &self.fetch)
            .field("rel", &self.rel_type_name)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for OrderedTwoHopPathCountExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OrderedTwoHopPathCountExec: rel={}, fetch={}",
            self.rel_type_name, self.fetch
        )
    }
}

impl ExecutionPlan for OrderedTwoHopPathCountExec {
    fn name(&self) -> &str {
        "OrderedTwoHopPathCountExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "OrderedTwoHopPathCountExec has no children".into(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "OrderedTwoHopPathCountExec only has partition 0, got {partition}"
            )));
        }
        let mut inbound = crate::adjacency::AdjacencyReader::new(
            self.provider.as_ref(),
            &self.rel_type_name,
            Direction::In,
        )
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let node_extent = inbound.node_extent();

        let mut remaining = self.fetch;
        let mut uuids = Vec::new();
        let mut candidates = 0_u64;
        let epoch = self.capture_epoch;
        for destination in 0..node_extent {
            if remaining == 0 {
                break;
            }
            demand::record_adjacency_row(self.capture_epoch, 1);
            let (path_count, examined) = count_two_hop_paths_to(
                &mut inbound,
                destination,
                self.require_edge_disjoint,
                remaining,
            )
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
            candidates = candidates.saturating_add(examined);
            if path_count == 0 {
                continue;
            }
            let lookup = self
                .ordinal_identities
                .lookup_node_uuids(&[destination])
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            demand::record_identity_projection(epoch, 1, 1, 1, &lookup.metrics);
            let uuid = *lookup.values[0]
                .as_ref()
                .ok_or_else(|| DataFusionError::Internal("missing destination uuid".into()))?
                .as_bytes();
            let emit = usize_from_u64(path_count).min(remaining);
            uuids.extend(std::iter::repeat_n(uuid, emit));
            remaining -= emit;
        }

        demand::record_candidates(epoch, 1, usize_from_u64(candidates));
        demand::record_emitted(epoch, 1, uuids.len());

        let mut builder = FixedSizeBinaryBuilder::with_capacity(uuids.len(), 16);
        for uuid in uuids {
            builder
                .append_value(uuid)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
        }
        let column: ArrayRef = Arc::new(builder.finish());
        let batch =
            arrow::record_batch::RecordBatch::try_new(Arc::clone(&self.schema), vec![column])
                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
        Ok(Box::pin(OrderedTwoHopStream {
            schema: Arc::clone(&self.schema),
            batch: Some(batch),
        }))
    }
}

fn usize_from_u64(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

fn count_two_hop_paths_to(
    inbound: &mut crate::adjacency::AdjacencyReader<'_>,
    destination: u64,
    require_edge_disjoint: bool,
    remaining: usize,
) -> std::result::Result<(u64, u64), graphforge_core::GfError> {
    const CHUNK: usize = 256;
    let mut count = 0_u64;
    let mut examined = 0_u64;
    let mut outer_offset = 0;
    loop {
        let outer = inbound.neighbor_chunk(destination, outer_offset, CHUNK)?;
        if outer.is_empty() {
            return Ok((count, examined));
        }
        outer_offset += outer.len();
        for (r2, middle) in outer {
            let mut inner_offset = 0;
            loop {
                let inner = inbound.neighbor_chunk(middle, inner_offset, CHUNK)?;
                if inner.is_empty() {
                    break;
                }
                inner_offset += inner.len();
                for (r1, _) in inner {
                    examined = examined.saturating_add(1);
                    if !require_edge_disjoint || r1 != r2 {
                        count = count.saturating_add(1);
                        if count >= remaining as u64 {
                            return Ok((count, examined));
                        }
                    }
                }
            }
        }
    }
}

struct OrderedTwoHopStream {
    schema: SchemaRef,
    batch: Option<arrow::record_batch::RecordBatch>,
}

impl Stream for OrderedTwoHopStream {
    type Item = Result<arrow::record_batch::RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.batch.take().map(Ok))
    }
}

impl RecordBatchStream for OrderedTwoHopStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
