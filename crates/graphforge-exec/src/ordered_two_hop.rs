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
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
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
    direction: Direction,
    provider: Arc<dyn AdjacencyProvider>,
    ordinal_identities: Option<Arc<crate::V4OrdinalIdentitySession>>,
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

fn detect_ordered_two_hop(plan: &Arc<dyn ExecutionPlan>) -> Option<OrderedTwoHopSpec> {
    let plan = peel_plan(plan);
    let projection = plan.downcast_ref::<ProjectionExec>()?;
    if projection.expr().len() != 1 {
        return None;
    }
    let sort = projection.children().first()?.downcast_ref::<SortExec>()?;
    let fetch = sort.fetch()?;
    if sort.expr().len() != 1 || sort.expr()[0].options.descending {
        return None;
    }
    let (expand2, require_edge_disjoint) =
        if let Some(filter) = sort.children().first()?.downcast_ref::<FilterExec>() {
            let disjoint = filter
                .predicate()
                .downcast_ref::<ScalarFunctionExpr>()
                .is_some_and(|function| function.name() == "cypher_relationship_disjoint");
            if !disjoint {
                return None;
            }
            let expand2 = filter.children().first()?.downcast_ref::<ExpandExec>()?;
            (expand2, true)
        } else {
            let expand2 = sort.children().first()?.downcast_ref::<ExpandExec>()?;
            (expand2, false)
        };
    let expand1 = expand2.children().first()?.downcast_ref::<ExpandExec>()?;
    if !expand1.is_destination_identity_only() || !expand2.is_destination_identity_only() {
        return None;
    }
    if expand1.rel_type_name() != expand2.rel_type_name()
        || expand1.direction() != expand2.direction()
    {
        return None;
    }
    Some(OrderedTwoHopSpec {
        schema: plan.schema(),
        props: Arc::clone(plan.properties()),
        fetch,
        rel_type_name: expand2.rel_type_name().to_owned(),
        direction: expand2.direction(),
        provider: Arc::clone(expand2.provider()),
        ordinal_identities: expand2.ordinal_identities(),
        require_edge_disjoint,
    })
}

pub struct OrderedTwoHopPathCountExec {
    schema: SchemaRef,
    props: Arc<PlanProperties>,
    fetch: usize,
    rel_type_name: String,
    direction: Direction,
    provider: Arc<dyn AdjacencyProvider>,
    ordinal_identities: Option<Arc<crate::V4OrdinalIdentitySession>>,
    require_edge_disjoint: bool,
}

impl OrderedTwoHopPathCountExec {
    fn new(spec: OrderedTwoHopSpec) -> Self {
        Self {
            schema: spec.schema,
            props: spec.props,
            fetch: spec.fetch,
            rel_type_name: spec.rel_type_name,
            direction: spec.direction,
            provider: spec.provider,
            ordinal_identities: spec.ordinal_identities,
            require_edge_disjoint: spec.require_edge_disjoint,
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
        let outbound = self
            .provider
            .adjacency(&self.rel_type_name, self.direction)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let inbound = self
            .provider
            .adjacency(&self.rel_type_name, Direction::In)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let mut max_node = 0_u64;
        outbound.for_each_row(|node_id, _| {
            max_node = max_node.max(node_id);
        });
        inbound.for_each_row(|node_id, _| {
            max_node = max_node.max(node_id);
        });

        let ordinal = self
            .ordinal_identities
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("ordinal identity required".into()))?;

        let mut remaining = self.fetch;
        let mut uuids = Vec::with_capacity(self.fetch);
        let mut candidates = 0_u64;
        for destination in 0..=max_node {
            if remaining == 0 {
                break;
            }
            let path_count =
                count_two_hop_paths_to(&inbound, destination, self.require_edge_disjoint);
            if path_count == 0 {
                continue;
            }
            candidates = candidates.saturating_add(path_count);
            let lookup = ordinal
                .lookup_node_uuids(&[destination])
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            let uuid = *lookup.values[0]
                .as_ref()
                .ok_or_else(|| DataFusionError::Internal("missing destination uuid".into()))?
                .as_bytes();
            let emit = usize_from_u64(path_count).min(remaining);
            uuids.extend(std::iter::repeat_n(uuid, emit));
            remaining -= emit;
        }

        demand::record_candidates(1, usize_from_u64(candidates));
        demand::record_emitted(1, uuids.len());

        let field = Field::new("id", DataType::FixedSizeBinary(16), true);
        let schema = Arc::new(Schema::new(vec![field]));
        let mut builder = FixedSizeBinaryBuilder::with_capacity(uuids.len(), 16);
        for uuid in uuids {
            builder
                .append_value(uuid)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
        }
        let column: ArrayRef = Arc::new(builder.finish());
        let batch = arrow::record_batch::RecordBatch::try_new(schema, vec![column])
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
    inbound: &crate::Adjacency,
    destination: u64,
    require_edge_disjoint: bool,
) -> u64 {
    let mut count = 0_u64;
    for (r2, middle) in inbound.neighbors(destination).iter() {
        for (r1, _) in inbound.neighbors(middle).iter() {
            if !require_edge_disjoint || r1 != r2 {
                count = count.saturating_add(1);
            }
        }
    }
    count
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
