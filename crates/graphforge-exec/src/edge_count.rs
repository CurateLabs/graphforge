//! CSR/catalog edge-count short-circuit for unconstrained `count(r)` (#1094).
//!
//! `MATCH ()-[r]->() RETURN count(r)` otherwise expands every adjacency entry
//! into Arrow batches before aggregating. That retains process RSS ~linear in
//! edge count and fails the progressive S18→S19 plateau gate. When the physical
//! plan is a global nonnull literal / compiler row-marker count over a single
//! unconstrained outward Expand (including a validated Partial/Final pair),
//! replace it with the adjacency view's edge-entry count.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::Stream;
use graphforge_ir::Direction;

use crate::ExpandExec;
use crate::adjacency::AdjacencyProvider;
use crate::demand;

/// Rewrite a global edge `count` over one Expand into an adjacency edge count.
pub(crate) fn try_rewrite_edge_count(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(spec) = detect_edge_count(&plan) else {
        return Ok(plan);
    };
    let replacement = Arc::new(EdgeCountExec::new(spec)) as Arc<dyn ExecutionPlan>;
    replace_peeled(&plan, replacement)
}

fn replace_peeled(
    plan: &Arc<dyn ExecutionPlan>,
    replacement: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(limit) = plan.downcast_ref::<GlobalLimitExec>() {
        return Arc::clone(plan)
            .with_new_children(vec![replace_peeled(limit.input(), replacement)?]);
    }
    if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() {
        return Arc::clone(plan)
            .with_new_children(vec![replace_peeled(coalesce.input(), replacement)?]);
    }
    if let Some(repartition) = plan.downcast_ref::<RepartitionExec>() {
        return Arc::clone(plan)
            .with_new_children(vec![replace_peeled(repartition.input(), replacement)?]);
    }
    Ok(replacement)
}

struct EdgeCountSpec {
    schema: SchemaRef,
    props: Arc<PlanProperties>,
    rel_type_name: String,
    direction: Direction,
    provider: Arc<dyn AdjacencyProvider>,
}

fn peel_transport(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if let Some(limit) = plan.downcast_ref::<GlobalLimitExec>() {
        return peel_transport(limit.input());
    }
    if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() {
        return peel_transport(coalesce.input());
    }
    if let Some(repartition) = plan.downcast_ref::<RepartitionExec>() {
        return peel_transport(repartition.input());
    }
    Arc::clone(plan)
}

fn peel_expand_input(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() {
        return peel_expand_input(coalesce.input());
    }
    if let Some(repartition) = plan.downcast_ref::<RepartitionExec>() {
        return peel_expand_input(repartition.input());
    }
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        return peel_expand_input(projection.input());
    }
    Arc::clone(plan)
}

/// Trace only row-preserving column projections/transports to an actual complete
/// topology scan. A node name or absence of Filter alone is not sufficient.
pub(crate) fn has_complete_frontier(expand: &ExpandExec) -> bool {
    fn trace(plan: &Arc<dyn ExecutionPlan>, column: usize) -> bool {
        if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
            return projection
                .expr()
                .get(column)
                .and_then(|expr| expr.expr.downcast_ref::<Column>())
                .is_some_and(|column| trace(projection.input(), column.index()));
        }
        if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() {
            return trace(coalesce.input(), column);
        }
        if let Some(repartition) = plan.downcast_ref::<RepartitionExec>() {
            return trace(repartition.input(), column);
        }
        graphforge_storage::parquet_scan::is_complete_node_id_scan(plan.as_ref(), column)
    }
    expand.fetch.is_none() && trace(&expand.input, expand.src_col_idx)
}

fn is_row_count(aggregate: &AggregateExec) -> bool {
    aggregate.group_expr().is_empty()
        && !aggregate.aggr_expr().is_empty()
        && aggregate.filter_expr().iter().all(Option::is_none)
        && aggregate.aggr_expr().iter().all(|expr| {
            if expr
                .fun()
                .inner()
                .downcast_ref::<datafusion::functions_aggregate::count::Count>()
                .is_none()
                || expr.is_distinct()
                || !expr.order_bys().is_empty()
            {
                return false;
            }
            expr.expressions().iter().all(|arg| {
                if let Some(literal) = arg.downcast_ref::<Literal>() {
                    return !literal.value().is_null();
                }
                arg.downcast_ref::<ScalarFunctionExpr>()
                    .is_some_and(|function| {
                        graphforge_rel::expr::is_cypher_row_marker(function.fun())
                            && function.args().len() == 1
                            && function.args()[0].downcast_ref::<Column>().is_some()
                    })
            })
        })
}

fn detect_edge_count(plan: &Arc<dyn ExecutionPlan>) -> Option<EdgeCountSpec> {
    let plan = peel_transport(plan);
    let aggregate = plan.downcast_ref::<AggregateExec>()?;
    if !is_row_count(aggregate) {
        return None;
    }
    let input = match aggregate.mode() {
        AggregateMode::Single => Arc::clone(aggregate.input()),
        AggregateMode::Final => {
            let input = peel_expand_input_without_projection(aggregate.input());
            let partial = input.downcast_ref::<AggregateExec>()?;
            if *partial.mode() != AggregateMode::Partial
                || !is_row_count(partial)
                || aggregate.aggr_expr() != partial.aggr_expr()
            {
                return None;
            }
            Arc::clone(partial.input())
        }
        _ => return None,
    };
    let expand_plan = peel_expand_input(&input);
    let expand = expand_plan.downcast_ref::<ExpandExec>()?;
    if expand.direction() != Direction::Out || !has_complete_frontier(expand) {
        return None;
    }
    Some(EdgeCountSpec {
        schema: plan.schema(),
        props: Arc::new(
            plan.properties()
                .as_ref()
                .clone()
                .with_partitioning(Partitioning::UnknownPartitioning(1)),
        ),
        rel_type_name: expand.rel_type_name().to_owned(),
        direction: expand.direction(),
        provider: Arc::clone(expand.provider()),
    })
}

fn peel_expand_input_without_projection(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() {
        return peel_expand_input_without_projection(coalesce.input());
    }
    if let Some(repartition) = plan.downcast_ref::<RepartitionExec>() {
        return peel_expand_input_without_projection(repartition.input());
    }
    Arc::clone(plan)
}

pub(crate) struct EdgeCountExec {
    schema: SchemaRef,
    props: Arc<PlanProperties>,
    rel_type_name: String,
    direction: Direction,
    provider: Arc<dyn AdjacencyProvider>,
    capture_epoch: u64,
}

impl EdgeCountExec {
    fn new(spec: EdgeCountSpec) -> Self {
        Self {
            schema: spec.schema,
            props: spec.props,
            rel_type_name: spec.rel_type_name,
            direction: spec.direction,
            provider: spec.provider,
            capture_epoch: demand::stamp_capture_epoch().unwrap_or(0),
        }
    }
}

impl fmt::Debug for EdgeCountExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EdgeCountExec")
            .field("rel", &self.rel_type_name)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for EdgeCountExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "EdgeCountExec: rel={}, dir={:?}",
            self.rel_type_name, self.direction
        )
    }
}

impl ExecutionPlan for EdgeCountExec {
    fn name(&self) -> &str {
        "EdgeCountExec"
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
                "EdgeCountExec has no children".into(),
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
                "EdgeCountExec only has partition 0, got {partition}"
            )));
        }
        let adjacency = self
            .provider
            .adjacency(&self.rel_type_name, self.direction)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let count = adjacency
            .edge_entry_count()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        // No edge candidates are materialized by the cardinality query.
        demand::record_candidates(self.capture_epoch, 0, 0);
        demand::record_emitted(self.capture_epoch, 0, 1);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for _ in self.schema.fields() {
            let value = i64::try_from(count)
                .map_err(|_| DataFusionError::Internal("edge count exceeds Int64".into()))?;
            columns.push(Arc::new(Int64Array::from(vec![value])));
        }
        let batch = arrow::record_batch::RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
        Ok(Box::pin(EdgeCountStream {
            schema: Arc::clone(&self.schema),
            batch: Some(batch),
        }))
    }
}

struct EdgeCountStream {
    schema: SchemaRef,
    batch: Option<arrow::record_batch::RecordBatch>,
}

impl Stream for EdgeCountStream {
    type Item = Result<arrow::record_batch::RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.batch.take().map(Ok))
    }
}

impl RecordBatchStream for EdgeCountStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
