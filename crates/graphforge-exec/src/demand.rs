//! Demand accounting and the final fixed-hop physical-plan rewrite (#1269).
//!
//! DataFusion correctly keeps a hard fetch above selective filters, but its
//! round-robin exchanges may eagerly buffer one full child batch per target
//! partition. This module supplies a soft batch goal and query cancellation to
//! the fixed-hop operators below that semantic boundary. Unknown and blocking
//! operators are deliberately opaque: demand never crosses them.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use datafusion::common::config::ConfigOptions;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::Stream;
use futures::task::AtomicWaker;

use crate::ExpandExec;

/// Per-hop deterministic work counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HopSnapshot {
    /// Upstream batches pulled by this hop.
    pub input_batches: u64,
    /// Upstream rows pulled by this hop.
    pub input_rows: u64,
    /// Adjacency candidates selected before record materialization.
    pub candidates_generated: u64,
    /// Rows emitted by the hop.
    pub rows_emitted: u64,
    /// Edge filtered reads scheduled.
    pub edge_reads_started: u64,
    /// Edge filtered reads completed.
    pub edge_reads_completed: u64,
    /// Edge filtered reads that failed after scheduling.
    pub edge_reads_failed: u64,
    /// Edge rows returned by filtered reads.
    pub edge_rows_returned: u64,
    /// Edge rows scanned by Parquet predicates.
    pub edge_rows_scanned: u64,
    /// Edge reads that used the full-read fallback.
    pub edge_full_reads: u64,
    /// Node filtered reads scheduled.
    pub node_reads_started: u64,
    /// Node filtered reads completed.
    pub node_reads_completed: u64,
    /// Node filtered reads that failed after scheduling.
    pub node_reads_failed: u64,
    /// Node rows returned by filtered reads.
    pub node_rows_returned: u64,
    /// Node rows scanned by Parquet predicates.
    pub node_rows_scanned: u64,
    /// Node reads that used the full-read fallback.
    pub node_full_reads: u64,
    /// Node reads that used exact dense row selection.
    pub node_dense_row_selection_reads: u64,
    /// Node reads that used conservative row-group pruning.
    pub node_row_group_predicate_reads: u64,
    /// Node row groups considered and selected by storage pruning.
    pub node_row_groups_considered: u64,
    /// Node row groups retained for decoding.
    pub node_row_groups_selected: u64,
    /// Node-id pages considered by exact dense selection.
    pub node_pages_considered: u64,
    /// Node-id pages containing selected rows.
    pub node_pages_selected: u64,
    /// Exact node row ordinals selected before membership validation.
    pub node_exact_rows_selected: u64,
    /// Dense selection attempts rejected by metadata validation.
    pub node_metadata_fallbacks: u64,
    /// Dense selection attempts rejected by output validation.
    pub node_validation_fallbacks: u64,
    /// Read attempts rejected after terminal cancellation.
    pub reads_after_cancel: u64,
}

/// Rows observed at one selective physical filter.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FilterSnapshot {
    /// Stable top-down ordinal within the bounded pipeline.
    pub ordinal: usize,
    /// Whether this is the relationship-uniqueness filter.
    pub relationship_uniqueness: bool,
    /// Rows entering the filter.
    pub input_rows: u64,
    /// Rows surviving the filter.
    pub output_rows: u64,
}

/// Aggregate-only diagnostic snapshot for the most recently reset capture.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DemandSnapshot {
    /// Per-hop counters keyed by the edge variable id.
    pub hops: BTreeMap<u32, HopSnapshot>,
    /// Selective filter counters.
    pub filters: BTreeMap<usize, FilterSnapshot>,
    /// Number of query cancellation signals.
    pub cancellations: u64,
    /// Maximum simultaneous filtered-read calls.
    pub max_in_flight_reads: u64,
}

static CAPTURE_ENABLED: AtomicBool = AtomicBool::new(false);
static CAPTURE: LazyLock<Mutex<DemandSnapshot>> =
    LazyLock::new(|| Mutex::new(DemandSnapshot::default()));

/// Reset and enable fixed-hop demand capture.
#[doc(hidden)]
pub fn reset() {
    *CAPTURE.lock().expect("demand stats lock") = DemandSnapshot::default();
    CAPTURE_ENABLED.store(true, Ordering::SeqCst);
}

/// Disable demand capture without discarding the last snapshot.
#[doc(hidden)]
pub fn disable() {
    CAPTURE_ENABLED.store(false, Ordering::SeqCst);
}

/// Copy the current fixed-hop demand counters.
#[must_use]
#[doc(hidden)]
pub fn snapshot() -> DemandSnapshot {
    CAPTURE.lock().expect("demand stats lock").clone()
}

pub(crate) fn capture_enabled() -> bool {
    CAPTURE_ENABLED.load(Ordering::Relaxed)
}

fn with_hop(edge_var: u32, update: impl FnOnce(&mut HopSnapshot)) {
    if !capture_enabled() {
        return;
    }
    let mut capture = CAPTURE.lock().expect("demand stats lock");
    update(capture.hops.entry(edge_var).or_default());
}

pub(crate) fn record_input(edge_var: u32, rows: usize) {
    with_hop(edge_var, |hop| {
        hop.input_batches += 1;
        hop.input_rows += rows as u64;
    });
}

pub(crate) fn record_candidates(edge_var: u32, rows: usize) {
    with_hop(edge_var, |hop| hop.candidates_generated += rows as u64);
}

pub(crate) fn record_emitted(edge_var: u32, rows: usize) {
    with_hop(edge_var, |hop| hop.rows_emitted += rows as u64);
}

fn record_filter(ordinal: usize, uniqueness: bool, input: bool, rows: usize) {
    if !capture_enabled() {
        return;
    }
    let mut capture = CAPTURE.lock().expect("demand stats lock");
    let filter = capture.filters.entry(ordinal).or_insert(FilterSnapshot {
        ordinal,
        relationship_uniqueness: uniqueness,
        ..FilterSnapshot::default()
    });
    if input {
        filter.input_rows += rows as u64;
    } else {
        filter.output_rows += rows as u64;
    }
}

/// Shared state attached to every fixed hop in one bounded physical plan.
pub(crate) struct QueryDemand {
    cancelled: AtomicBool,
    in_flight_reads: AtomicUsize,
    max_in_flight_reads: AtomicUsize,
    produced_rows: AtomicUsize,
    quiescent_waker: AtomicWaker,
}

impl QueryDemand {
    fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            in_flight_reads: AtomicUsize::new(0),
            max_in_flight_reads: AtomicUsize::new(0),
            produced_rows: AtomicUsize::new(0),
            quiescent_waker: AtomicWaker::new(),
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn cancel(&self) {
        if !self.cancelled.swap(true, Ordering::AcqRel) && capture_enabled() {
            CAPTURE.lock().expect("demand stats lock").cancellations += 1;
        }
    }

    fn observe_output(&self, rows: usize, cancel_after: usize) -> bool {
        let before = self.produced_rows.fetch_add(rows, Ordering::AcqRel);
        before.saturating_add(rows) >= cancel_after
    }

    pub(crate) fn begin_read(self: &Arc<Self>, edge_var: u32) -> Option<ReadPermit> {
        if self.is_cancelled() {
            with_hop(edge_var, |hop| hop.reads_after_cancel += 1);
            return None;
        }
        let current = self.in_flight_reads.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_in_flight_reads
            .fetch_max(current, Ordering::AcqRel);
        if capture_enabled() {
            let mut capture = CAPTURE.lock().expect("demand stats lock");
            capture.max_in_flight_reads = capture.max_in_flight_reads.max(current as u64);
        }
        if self.is_cancelled() {
            self.finish_read();
            with_hop(edge_var, |hop| hop.reads_after_cancel += 1);
            return None;
        }
        Some(ReadPermit {
            demand: Arc::clone(self),
        })
    }

    fn finish_read(&self) {
        if self.in_flight_reads.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.quiescent_waker.wake();
        }
    }

    fn poll_quiescent(&self, cx: &Context<'_>) -> Poll<()> {
        if self.in_flight_reads.load(Ordering::Acquire) == 0 {
            return Poll::Ready(());
        }
        self.quiescent_waker.register(cx.waker());
        if self.in_flight_reads.load(Ordering::Acquire) == 0 {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

pub(crate) struct ReadPermit {
    demand: Arc<QueryDemand>,
}

impl Drop for ReadPermit {
    fn drop(&mut self) {
        self.demand.finish_read();
    }
}

/// Attributes storage observer events to one fixed-hop edge binding.
pub(crate) struct HopReadObserver {
    edge_var: u32,
}

impl HopReadObserver {
    pub(crate) fn new(edge_var: u32) -> Self {
        Self { edge_var }
    }
}

impl graphforge_storage::io_stats::FilteredReadObserver for HopReadObserver {
    fn read_started(&self, table: graphforge_storage::io_stats::FilteredReadTable) {
        with_hop(self.edge_var, |hop| match table {
            graphforge_storage::io_stats::FilteredReadTable::Edge => hop.edge_reads_started += 1,
            graphforge_storage::io_stats::FilteredReadTable::Node => hop.node_reads_started += 1,
        });
    }

    fn rows_scanned(&self, table: graphforge_storage::io_stats::FilteredReadTable, rows: u64) {
        with_hop(self.edge_var, |hop| match table {
            graphforge_storage::io_stats::FilteredReadTable::Edge => hop.edge_rows_scanned += rows,
            graphforge_storage::io_stats::FilteredReadTable::Node => hop.node_rows_scanned += rows,
        });
    }

    fn read_completed(
        &self,
        table: graphforge_storage::io_stats::FilteredReadTable,
        rows: u64,
        full: bool,
    ) {
        with_hop(self.edge_var, |hop| match table {
            graphforge_storage::io_stats::FilteredReadTable::Edge => {
                hop.edge_reads_completed += 1;
                hop.edge_rows_returned += rows;
                hop.edge_full_reads += u64::from(full);
            }
            graphforge_storage::io_stats::FilteredReadTable::Node => {
                hop.node_reads_completed += 1;
                hop.node_rows_returned += rows;
                hop.node_full_reads += u64::from(full);
            }
        });
    }

    fn read_failed(&self, table: graphforge_storage::io_stats::FilteredReadTable) {
        with_hop(self.edge_var, |hop| match table {
            graphforge_storage::io_stats::FilteredReadTable::Edge => hop.edge_reads_failed += 1,
            graphforge_storage::io_stats::FilteredReadTable::Node => hop.node_reads_failed += 1,
        });
    }

    fn pruning(
        &self,
        table: graphforge_storage::io_stats::FilteredReadTable,
        pruning: graphforge_storage::io_stats::FilteredReadPruning,
    ) {
        if table != graphforge_storage::io_stats::FilteredReadTable::Node {
            return;
        }
        with_hop(self.edge_var, |hop| {
            match pruning.strategy {
                graphforge_storage::io_stats::FilteredReadStrategy::DenseRowSelection => {
                    hop.node_dense_row_selection_reads += 1;
                }
                graphforge_storage::io_stats::FilteredReadStrategy::RowGroupPredicate => {
                    hop.node_row_group_predicate_reads += 1;
                }
                graphforge_storage::io_stats::FilteredReadStrategy::FullFallback => {}
            }
            hop.node_row_groups_considered += pruning.row_groups_considered;
            hop.node_row_groups_selected += pruning.row_groups_selected;
            hop.node_pages_considered += pruning.pages_considered;
            hop.node_pages_selected += pruning.pages_selected;
            hop.node_exact_rows_selected += pruning.exact_rows_selected;
            hop.node_metadata_fallbacks += pruning.metadata_fallbacks;
            hop.node_validation_fallbacks += pruning.validation_fallbacks;
        });
    }
}

/// Final physical optimizer rule for bounded fixed-hop pipelines.
#[derive(Debug, Default)]
pub(crate) struct FixedHopDemandRule;

#[derive(Clone, Copy)]
struct TerminalDemand {
    batch_goal: usize,
    cancel_after: usize,
}

impl PhysicalOptimizerRule for FixedHopDemandRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(terminal) = find_terminal_demand(&plan) else {
            return Ok(plan);
        };
        if !contains_demand_expand(&plan) {
            return Ok(plan);
        }
        let demand = Arc::new(QueryDemand::new());
        if terminal.cancel_after == 0 {
            demand.cancel();
        }
        let mut filter_ordinal = 0;
        let initial_batch_goal = terminal.batch_goal.min(config.execution.batch_size);
        let rewritten = rewrite_bounded(
            plan,
            initial_batch_goal,
            Arc::clone(&demand),
            &mut filter_ordinal,
        )?;
        Ok(Arc::new(DemandGuardExec::new(
            rewritten,
            demand,
            terminal.cancel_after,
        )))
    }

    fn name(&self) -> &str {
        "fixed_hop_demand"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn find_terminal_demand(plan: &Arc<dyn ExecutionPlan>) -> Option<TerminalDemand> {
    if let Some(limit) = plan.as_any().downcast_ref::<GlobalLimitExec>() {
        let fetch = limit.fetch()?;
        return Some(TerminalDemand {
            batch_goal: limit.skip().saturating_add(fetch),
            cancel_after: fetch,
        });
    }
    if is_fetch_transparent(plan.as_ref())
        && let Some(fetch) = plan.fetch()
    {
        return Some(TerminalDemand {
            batch_goal: fetch,
            cancel_after: fetch,
        });
    }
    if is_fetch_transparent(plan.as_ref()) {
        let children = plan.children();
        if children.len() == 1 {
            return find_terminal_demand(children[0]);
        }
    }
    None
}

fn is_fetch_transparent(plan: &dyn ExecutionPlan) -> bool {
    plan.as_any().is::<GlobalLimitExec>()
        || plan.as_any().is::<LocalLimitExec>()
        || plan.as_any().is::<CoalescePartitionsExec>()
        || plan.as_any().is::<FilterExec>()
        || plan.as_any().is::<ProjectionExec>()
        || plan.as_any().is::<RepartitionExec>()
        || plan.as_any().is::<ExpandExec>()
}

/// Whether a fixed hop is reachable without crossing a semantic boundary.
/// This is intentionally stricter than a whole-tree search: an exchange above
/// a sort/join/aggregate must not be treated as part of the bounded pipeline.
fn contains_demand_expand(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if plan.as_any().is::<ExpandExec>() {
        return true;
    }
    is_fetch_transparent(plan.as_ref()) && plan.children().into_iter().any(contains_demand_expand)
}

fn rewrite_bounded(
    plan: Arc<dyn ExecutionPlan>,
    batch_goal: usize,
    demand: Arc<QueryDemand>,
    filter_ordinal: &mut usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(repartition) = plan.as_any().downcast_ref::<RepartitionExec>()
        && !repartition.preserve_order()
        && matches!(repartition.partitioning(), Partitioning::RoundRobinBatch(_))
        && contains_demand_expand(repartition.input())
    {
        return rewrite_bounded(
            Arc::clone(repartition.input()),
            batch_goal,
            demand,
            filter_ordinal,
        );
    }

    if !is_fetch_transparent(plan.as_ref()) {
        return Ok(plan); // fail closed at blockers and unknown nodes
    }

    if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
        let ordinal = *filter_ordinal;
        *filter_ordinal += 1;
        let uniqueness = filter
            .predicate()
            .as_any()
            .downcast_ref::<ScalarFunctionExpr>()
            .is_some_and(|function| function.name() == "cypher_relationship_disjoint");
        let child = rewrite_bounded(
            Arc::clone(filter.input()),
            batch_goal,
            Arc::clone(&demand),
            filter_ordinal,
        )?;
        let child = if capture_enabled() {
            Arc::new(ProbeExec::new(child, ordinal, uniqueness, true)) as _
        } else {
            child
        };
        let rebuilt = Arc::clone(&plan).with_new_children(vec![child])?;
        return if capture_enabled() {
            Ok(Arc::new(ProbeExec::new(
                rebuilt, ordinal, uniqueness, false,
            )))
        } else {
            Ok(rebuilt)
        };
    }

    let children = plan.children();
    let mut rewritten_children = Vec::with_capacity(children.len());
    for child in children {
        rewritten_children.push(rewrite_bounded(
            Arc::clone(child),
            batch_goal,
            Arc::clone(&demand),
            filter_ordinal,
        )?);
    }
    let rebuilt = if rewritten_children.is_empty() {
        plan
    } else {
        plan.with_new_children(rewritten_children)?
    };
    if let Some(expand) = rebuilt.as_any().downcast_ref::<ExpandExec>() {
        return Ok(expand.with_demand(batch_goal, demand));
    }
    Ok(rebuilt)
}

struct ProbeExec {
    input: Arc<dyn ExecutionPlan>,
    ordinal: usize,
    uniqueness: bool,
    input_side: bool,
    props: Arc<PlanProperties>,
}

impl ProbeExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        ordinal: usize,
        uniqueness: bool,
        input_side: bool,
    ) -> Self {
        let props = Arc::clone(input.properties());
        Self {
            input,
            ordinal,
            uniqueness,
            input_side,
            props,
        }
    }
}

impl fmt::Debug for ProbeExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DemandProbeExec")
            .field("ordinal", &self.ordinal)
            .field("input_side", &self.input_side)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for ProbeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DemandProbeExec: filter={}, side={}",
            self.ordinal,
            if self.input_side { "input" } else { "output" }
        )
    }
}

impl ExecutionPlan for ProbeExec {
    fn name(&self) -> &str {
        "DemandProbeExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = children
            .pop()
            .ok_or_else(|| DataFusionError::Internal("DemandProbeExec needs one child".into()))?;
        Ok(Arc::new(Self::new(
            input,
            self.ordinal,
            self.uniqueness,
            self.input_side,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        Ok(Box::pin(ProbeStream {
            schema: stream.schema(),
            inner: stream,
            ordinal: self.ordinal,
            uniqueness: self.uniqueness,
            input_side: self.input_side,
        }))
    }
}

struct ProbeStream {
    schema: SchemaRef,
    inner: SendableRecordBatchStream,
    ordinal: usize,
    uniqueness: bool,
    input_side: bool,
}

impl Stream for ProbeStream {
    type Item = Result<arrow::array::RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                record_filter(
                    self.ordinal,
                    self.uniqueness,
                    self.input_side,
                    batch.num_rows(),
                );
                Poll::Ready(Some(Ok(batch)))
            }
            other => other,
        }
    }
}

impl RecordBatchStream for ProbeStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

struct DemandGuardExec {
    input: Arc<dyn ExecutionPlan>,
    demand: Arc<QueryDemand>,
    cancel_after: usize,
    props: Arc<PlanProperties>,
}

impl DemandGuardExec {
    fn new(input: Arc<dyn ExecutionPlan>, demand: Arc<QueryDemand>, cancel_after: usize) -> Self {
        let props = Arc::clone(input.properties());
        Self {
            input,
            demand,
            cancel_after,
            props,
        }
    }
}

impl fmt::Debug for DemandGuardExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DemandGuardExec")
            .field("cancel_after", &self.cancel_after)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for DemandGuardExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DemandGuardExec: cancel_after={}", self.cancel_after)
    }
}

impl ExecutionPlan for DemandGuardExec {
    fn name(&self) -> &str {
        "DemandGuardExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = children
            .pop()
            .ok_or_else(|| DataFusionError::Internal("DemandGuardExec needs one child".into()))?;
        Ok(Arc::new(Self::new(
            input,
            Arc::clone(&self.demand),
            self.cancel_after,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if self.cancel_after == 0 {
            return Ok(Box::pin(
                datafusion::physical_plan::stream::EmptyRecordBatchStream::new(self.schema()),
            ));
        }
        let stream = self.input.execute(partition, context)?;
        Ok(Box::pin(DemandGuardStream {
            schema: stream.schema(),
            inner: Some(stream),
            demand: Arc::clone(&self.demand),
            cancel_after: self.cancel_after,
            finishing: false,
        }))
    }
}

struct DemandGuardStream {
    schema: SchemaRef,
    inner: Option<SendableRecordBatchStream>,
    demand: Arc<QueryDemand>,
    cancel_after: usize,
    finishing: bool,
}

impl Stream for DemandGuardStream {
    type Item = Result<arrow::array::RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finishing {
            return self.demand.poll_quiescent(cx).map(|()| None);
        }
        let Some(inner) = self.inner.as_mut() else {
            return Poll::Ready(None);
        };
        match Pin::new(inner).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                if self
                    .demand
                    .observe_output(batch.num_rows(), self.cancel_after)
                {
                    self.demand.cancel();
                    self.inner.take();
                    self.finishing = true;
                }
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(error))) => {
                self.demand.cancel();
                self.inner.take();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.demand.cancel();
                self.inner.take();
                self.finishing = true;
                self.demand.poll_quiescent(cx).map(|()| None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for DemandGuardStream {
    fn drop(&mut self) {
        self.demand.cancel();
        self.inner.take();
    }
}

impl RecordBatchStream for DemandGuardStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::task::Context;

    use arrow::datatypes::Schema;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::limit::GlobalLimitExec;
    use datafusion::physical_plan::stream::EmptyRecordBatchStream;

    use super::*;

    #[test]
    fn capture_accounts_for_every_hop_filter_and_storage_outcome() {
        use graphforge_storage::io_stats::{
            FilteredReadObserver, FilteredReadPruning, FilteredReadStrategy, FilteredReadTable,
        };

        reset();
        record_input(12, 3);
        record_candidates(12, 7);
        record_emitted(12, 2);
        record_filter(4, true, true, 7);
        record_filter(4, true, false, 2);

        let observer = HopReadObserver::new(12);
        for table in [FilteredReadTable::Edge, FilteredReadTable::Node] {
            observer.read_started(table);
            observer.rows_scanned(table, 11);
            observer.read_completed(table, 5, true);
            observer.read_failed(table);
        }
        for strategy in [
            FilteredReadStrategy::DenseRowSelection,
            FilteredReadStrategy::RowGroupPredicate,
            FilteredReadStrategy::FullFallback,
        ] {
            observer.pruning(
                FilteredReadTable::Node,
                FilteredReadPruning {
                    strategy,
                    row_groups_considered: 9,
                    row_groups_selected: 4,
                    pages_considered: 8,
                    pages_selected: 3,
                    exact_rows_selected: 2,
                    metadata_fallbacks: 1,
                    validation_fallbacks: 1,
                },
            );
        }
        observer.pruning(
            FilteredReadTable::Edge,
            FilteredReadPruning {
                strategy: FilteredReadStrategy::DenseRowSelection,
                row_groups_considered: 99,
                row_groups_selected: 99,
                pages_considered: 99,
                pages_selected: 99,
                exact_rows_selected: 99,
                metadata_fallbacks: 99,
                validation_fallbacks: 99,
            },
        );

        let captured = snapshot();
        let hop = &captured.hops[&12];
        assert_eq!((hop.input_batches, hop.input_rows), (1, 3));
        assert_eq!((hop.candidates_generated, hop.rows_emitted), (7, 2));
        assert_eq!(
            (
                hop.edge_reads_started,
                hop.edge_reads_completed,
                hop.edge_reads_failed
            ),
            (1, 1, 1)
        );
        assert_eq!(
            (
                hop.node_reads_started,
                hop.node_reads_completed,
                hop.node_reads_failed
            ),
            (1, 1, 1)
        );
        assert_eq!(
            (
                hop.edge_rows_scanned,
                hop.edge_rows_returned,
                hop.edge_full_reads
            ),
            (11, 5, 1)
        );
        assert_eq!(
            (
                hop.node_rows_scanned,
                hop.node_rows_returned,
                hop.node_full_reads
            ),
            (11, 5, 1)
        );
        assert_eq!(
            (
                hop.node_dense_row_selection_reads,
                hop.node_row_group_predicate_reads
            ),
            (1, 1)
        );
        assert_eq!(
            (hop.node_row_groups_considered, hop.node_row_groups_selected),
            (27, 12)
        );
        assert_eq!(
            (hop.node_pages_considered, hop.node_pages_selected),
            (24, 9)
        );
        assert_eq!(
            (
                hop.node_exact_rows_selected,
                hop.node_metadata_fallbacks,
                hop.node_validation_fallbacks
            ),
            (6, 3, 3)
        );
        assert_eq!(
            (
                captured.filters[&4].input_rows,
                captured.filters[&4].output_rows
            ),
            (7, 2)
        );

        disable();
        record_input(12, 100);
        assert_eq!(snapshot(), captured);
    }

    #[test]
    fn quiescence_tracks_live_permits_and_output_thresholds() {
        let demand = Arc::new(QueryDemand::new());
        assert!(!demand.observe_output(2, 3));
        assert!(demand.observe_output(1, 3));

        let permit = demand.begin_read(1).unwrap();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(demand.poll_quiescent(&mut cx), Poll::Pending));
        drop(permit);
        assert!(matches!(demand.poll_quiescent(&mut cx), Poll::Ready(())));
    }

    #[test]
    fn read_permit_double_check_rejects_post_cancellation_work() {
        let demand = Arc::new(QueryDemand::new());
        let permit = demand.begin_read(7).expect("first read is permitted");
        assert_eq!(demand.in_flight_reads.load(Ordering::Acquire), 1);
        assert_eq!(demand.max_in_flight_reads.load(Ordering::Acquire), 1);
        drop(permit);
        assert_eq!(demand.in_flight_reads.load(Ordering::Acquire), 0);
        demand.cancel();
        assert!(demand.is_cancelled());
        assert!(demand.begin_read(7).is_none());
    }

    #[test]
    fn dropping_terminal_stream_cancels_chain() {
        let demand = Arc::new(QueryDemand::new());
        let schema = Arc::new(Schema::empty());
        let stream = DemandGuardStream {
            schema: Arc::clone(&schema),
            inner: Some(Box::pin(EmptyRecordBatchStream::new(schema))),
            demand: Arc::clone(&demand),
            cancel_after: 10,
            finishing: false,
        };
        drop(stream);
        assert!(demand.is_cancelled());
    }

    #[test]
    fn limited_non_traversal_plan_is_not_wrapped() {
        let schema = Arc::new(Schema::empty());
        let empty: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(empty, 0, Some(10)));
        let optimized = FixedHopDemandRule
            .optimize(Arc::clone(&plan), &ConfigOptions::new())
            .unwrap();
        assert!(Arc::ptr_eq(&plan, &optimized));
    }

    #[tokio::test]
    async fn demand_wrappers_replace_children_and_enforce_zero_demand() {
        use datafusion::physical_plan::collect;
        use datafusion::prelude::SessionContext;

        let schema = Arc::new(Schema::empty());
        let child: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let replacement: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));

        let probe = Arc::new(ProbeExec::new(Arc::clone(&child), 4, true, false));
        let probe_error = Arc::clone(&probe)
            .with_new_children(vec![])
            .expect_err("probe requires one child");
        assert!(matches!(
            probe_error,
            DataFusionError::Internal(message) if message == "DemandProbeExec needs one child"
        ));
        let replaced_probe = probe
            .with_new_children(vec![Arc::clone(&replacement)])
            .expect("replace probe child");
        assert!(Arc::ptr_eq(replaced_probe.children()[0], &replacement));

        let demand = Arc::new(QueryDemand::new());
        let guard = Arc::new(DemandGuardExec::new(
            Arc::clone(&child),
            Arc::clone(&demand),
            0,
        ));
        let guard_error = Arc::clone(&guard)
            .with_new_children(vec![])
            .expect_err("guard requires one child");
        assert!(matches!(
            guard_error,
            DataFusionError::Internal(message) if message == "DemandGuardExec needs one child"
        ));
        let replaced_guard = guard
            .with_new_children(vec![replacement])
            .expect("replace guard child");
        assert_eq!(replaced_guard.name(), "DemandGuardExec");

        let context = SessionContext::new();
        let batches = collect(replaced_guard, context.task_ctx())
            .await
            .expect("zero-demand guard returns an empty stream");
        assert!(batches.is_empty());
        assert!(
            !demand.is_cancelled(),
            "zero demand does not start the child"
        );
    }

    #[tokio::test]
    async fn exhausted_child_cancels_demand_and_finishes_quiescently() {
        use datafusion::physical_plan::collect;
        use datafusion::prelude::SessionContext;

        let schema = Arc::new(Schema::empty());
        let child: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let demand = Arc::new(QueryDemand::new());
        let guard: Arc<dyn ExecutionPlan> =
            Arc::new(DemandGuardExec::new(child, Arc::clone(&demand), 10));
        let batches = collect(guard, SessionContext::new().task_ctx())
            .await
            .expect("empty child finishes normally");
        assert!(batches.is_empty());
        assert!(demand.is_cancelled());
        assert_eq!(demand.in_flight_reads.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn demand_wrappers_expose_diagnostics_and_cancel_on_child_error() {
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures::StreamExt;

        let schema = Arc::new(Schema::empty());
        let child: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let probe = ProbeExec::new(Arc::clone(&child), 3, true, true);
        assert_eq!(probe.name(), "DemandProbeExec");
        assert!(probe.as_any().is::<ProbeExec>());
        assert!(format!("{probe:?}").contains("ordinal: 3"));
        assert!(
            format!(
                "{}",
                datafusion::physical_plan::displayable(&probe).one_line()
            )
            .contains("side=input")
        );
        assert_eq!(probe.children().len(), 1);
        assert_eq!(probe.schema(), schema);

        let demand = Arc::new(QueryDemand::new());
        let guard = DemandGuardExec::new(child, Arc::clone(&demand), 2);
        assert!(guard.as_any().is::<DemandGuardExec>());
        assert!(format!("{guard:?}").contains("cancel_after: 2"));
        assert!(
            format!(
                "{}",
                datafusion::physical_plan::displayable(&guard).one_line()
            )
            .contains("cancel_after=2")
        );

        let error = DataFusionError::Execution("sentinel child failure".into());
        let inner = RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Err(error)]),
        );
        let mut stream = DemandGuardStream {
            schema: Arc::clone(&schema),
            inner: Some(Box::pin(inner)),
            demand: Arc::clone(&demand),
            cancel_after: 2,
            finishing: false,
        };
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(error.to_string().contains("sentinel child failure"));
        assert!(demand.is_cancelled());
        assert!(stream.inner.is_none());
        assert_eq!(stream.schema(), schema);
    }
}
