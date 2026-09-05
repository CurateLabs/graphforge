//! Order-aware one-hop emission for `ORDER BY destination_uuid LIMIT K` (#1094).
//!
//! Canonical ladder one-hop queries are identity-only expands into TopK Sort.
//! Streaming every adjacency entry before the limit retains process RSS ~linear
//! in edge count. Mirror the two-hop rewrite: walk destinations in node-id
//! order (UUID order for monotonic ordinal identity), emit each destination's
//! inbound multiplicity until the limit is satisfied, then stop.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, FixedSizeBinaryBuilder};
use arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
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

/// Rewrite ordered one-hop identity-only plans when matched.
pub fn try_rewrite_ordered_one_hop(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(spec) = detect_ordered_one_hop(&plan) else {
        return Ok(plan);
    };
    let replacement = Arc::new(OrderedOneHopExec::new(spec)) as Arc<dyn ExecutionPlan>;
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

struct OrderedOneHopSpec {
    schema: SchemaRef,
    props: Arc<PlanProperties>,
    fetch: usize,
    rel_type_name: String,
    direction: Direction,
    provider: Arc<dyn AdjacencyProvider>,
    ordinal_identities: Arc<crate::V4OrdinalIdentitySession>,
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

fn detect_ordered_one_hop(plan: &Arc<dyn ExecutionPlan>) -> Option<OrderedOneHopSpec> {
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
    let expand_plan = peel_expand_transport(sort.children().first()?);
    let expand = expand_plan.downcast_ref::<ExpandExec>()?;
    // Two-hop plans are handled separately; refuse stacked expands here.
    if expand.children().first().is_some_and(|child| {
        let peeled = peel_expand_transport(child);
        peeled.downcast_ref::<ExpandExec>().is_some()
    }) {
        return None;
    }
    if !expand.is_destination_identity_only() || expand.direction() != Direction::Out {
        return None;
    }
    let ordinal_identities = expand.ordinal_identities()?;
    if !crate::edge_count::has_complete_frontier(expand)
        || !ordinal_identities.uuid_order_matches_ordinals()
    {
        return None;
    }
    Some(OrderedOneHopSpec {
        schema: plan.schema(),
        props: Arc::new(
            plan.properties()
                .as_ref()
                .clone()
                .with_partitioning(Partitioning::UnknownPartitioning(1)),
        ),
        fetch,
        rel_type_name: expand.rel_type_name().to_owned(),
        direction: expand.direction(),
        provider: Arc::clone(expand.provider()),
        ordinal_identities,
    })
}

pub struct OrderedOneHopExec {
    schema: SchemaRef,
    props: Arc<PlanProperties>,
    fetch: usize,
    rel_type_name: String,
    direction: Direction,
    provider: Arc<dyn AdjacencyProvider>,
    ordinal_identities: Arc<crate::V4OrdinalIdentitySession>,
    capture_epoch: u64,
}

impl OrderedOneHopExec {
    fn new(spec: OrderedOneHopSpec) -> Self {
        Self {
            schema: spec.schema,
            props: spec.props,
            fetch: spec.fetch,
            rel_type_name: spec.rel_type_name,
            direction: spec.direction,
            provider: spec.provider,
            ordinal_identities: spec.ordinal_identities,
            capture_epoch: demand::stamp_capture_epoch().unwrap_or(0),
        }
    }
}

impl fmt::Debug for OrderedOneHopExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderedOneHopExec")
            .field("fetch", &self.fetch)
            .field("rel", &self.rel_type_name)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for OrderedOneHopExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OrderedOneHopExec: rel={}, dir={:?}, fetch={}",
            self.rel_type_name, self.direction, self.fetch
        )
    }
}

impl ExecutionPlan for OrderedOneHopExec {
    fn name(&self) -> &str {
        "OrderedOneHopExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "OrderedOneHopExec has no children".into(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "OrderedOneHopExec only has partition 0, got {partition}"
            )));
        }
        // Inbound view: destinations ordered by node id, multiplicity = in-degree.
        let inbound = self
            .provider
            .adjacency(&self.rel_type_name, Direction::In)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let node_extent = inbound.node_extent();

        let mut remaining = self.fetch;
        let mut destinations = Vec::new();
        let mut candidates = 0_u64;
        let epoch = self.capture_epoch;
        for destination in 0..node_extent {
            if remaining == 0 {
                break;
            }
            demand::record_adjacency_row(self.capture_epoch, 0);
            let in_degree = inbound
                .degree(destination)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            if in_degree == 0 {
                continue;
            }
            let emit = usize_from_u64(in_degree).min(remaining);
            candidates = candidates.saturating_add(emit as u64);
            destinations.push((destination, emit));
            remaining -= emit;
        }

        demand::record_candidates(epoch, 0, usize_from_u64(candidates));
        demand::record_emitted(epoch, 0, self.fetch - remaining);

        let mut builder = FixedSizeBinaryBuilder::with_capacity(self.fetch - remaining, 16);
        for destinations in destinations.chunks(self.ordinal_identities.max_requested_ids()) {
            let requested = destinations.iter().map(|(id, _)| *id).collect::<Vec<_>>();
            let lookup = self
                .ordinal_identities
                .lookup_node_uuids(&requested)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            demand::record_identity_projection(epoch, 0, requested.len(), 1, &lookup.metrics);
            for ((_, copies), uuid) in destinations.iter().zip(&lookup.values) {
                let uuid = uuid
                    .as_ref()
                    .ok_or_else(|| DataFusionError::Internal("missing destination uuid".into()))?;
                for _ in 0..*copies {
                    builder
                        .append_value(uuid.as_bytes())
                        .map_err(|error| DataFusionError::External(Box::new(error)))?;
                }
            }
        }
        let column: ArrayRef = Arc::new(builder.finish());
        let batch =
            arrow::record_batch::RecordBatch::try_new(Arc::clone(&self.schema), vec![column])
                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
        Ok(Box::pin(OrderedOneHopStream {
            schema: Arc::clone(&self.schema),
            batch: Some(batch),
        }))
    }
}

fn usize_from_u64(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

struct OrderedOneHopStream {
    schema: SchemaRef,
    batch: Option<arrow::record_batch::RecordBatch>,
}

impl Stream for OrderedOneHopStream {
    type Item = Result<arrow::record_batch::RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.batch.take().map(Ok))
    }
}

impl RecordBatchStream for OrderedOneHopStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
