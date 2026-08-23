//! Demand accounting and the final fixed-hop physical-plan rewrite (#1269).
//!
//! DataFusion correctly keeps a hard fetch above selective filters, but its
//! round-robin exchanges may eagerly buffer one full child batch per target
//! partition. This module supplies a soft batch goal and query cancellation to
//! the fixed-hop operators below that semantic boundary. Unknown and blocking
//! operators are deliberately opaque: demand never crosses them.

use std::collections::BTreeMap;
use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
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
use datafusion::physical_plan::sorts::sort::SortExec;
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
    /// Blocking ordered operators, in stable top-down plan order.
    pub sorts: Vec<SortSnapshot>,
    /// Query memory-pool reservation before physical execution.
    pub memory_reserved_before: u64,
    /// Query memory-pool reservation after every stream/operator was dropped.
    pub memory_reserved_after: u64,
    /// Arrow bytes retained by returned batches at the post-operator boundary.
    pub returned_batch_bytes: u64,
    /// Process RSS attributed to operator lifetimes by the query sampler.
    pub operator_rss: OperatorRssSnapshot,
}

/// Process-memory evidence sampled while blocking operators were alive.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OperatorRssSnapshot {
    /// Highest RSS sample while at least one expand stream was alive.
    pub expand_peak_bytes: u64,
    /// Last RSS sample while an expand stream was alive.
    pub expand_current_bytes: u64,
    /// Highest RSS sample while a plan containing a sort was collecting.
    pub sort_peak_bytes: u64,
    /// Last RSS sample while a plan containing a sort was collecting.
    pub sort_current_bytes: u64,
    /// Per-hop RSS lifetime evidence keyed by edge variable.
    pub expand_by_hop: BTreeMap<u32, RssLifetimeSnapshot>,
    /// RSS sampled while sort collection was active and no expansion stream
    /// was active. This is the non-overlapping ordered-operator attribution.
    pub sort_exclusive: RssLifetimeSnapshot,
}

/// Process RSS at the boundaries and peak of one operator lifetime.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RssLifetimeSnapshot {
    /// RSS when the first matching operator became active.
    pub before_bytes: u64,
    /// Highest RSS sampled while the operator was active.
    pub peak_bytes: u64,
    /// Last RSS sampled while the operator was active.
    pub current_bytes: u64,
    /// RSS after the last matching operator was dropped.
    pub after_bytes: u64,
}

/// Authoritative post-execution DataFusion metrics for one ordered operator.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SortSnapshot {
    /// Stable top-down ordinal in the physical plan.
    pub ordinal: usize,
    /// Hard TopK row bound. `None` means the spillable external sorter path.
    pub fetch: Option<usize>,
    /// Rows emitted by this sort.
    pub output_rows: u64,
    /// Output record batches emitted by this sort.
    pub output_batches: u64,
    /// External-sort spill count (zero for TopK).
    pub spill_count: u64,
    /// External-sort bytes spilled (zero for TopK).
    pub spilled_bytes: u64,
    /// Memory still reserved when execution completed; this must quiesce to zero.
    pub memory_used_after: u64,
}

tokio::task_local! { static ACTIVE_CAPTURE: Arc<QueryCapture>; }

struct QueryCapture {
    snapshot: Mutex<DemandSnapshot>,
    expand_active: AtomicUsize,
    sort_active: AtomicUsize,
    expand_peak: AtomicU64,
    expand_current: AtomicU64,
    sort_peak: AtomicU64,
    sort_current: AtomicU64,
    expand_lifetimes: Mutex<BTreeMap<u32, ActiveRssLifetime>>,
    sort_exclusive: Mutex<ActiveRssLifetime>,
    stop: AtomicBool,
}

impl QueryCapture {
    fn new() -> Self {
        Self {
            snapshot: Mutex::new(DemandSnapshot::default()),
            expand_active: AtomicUsize::new(0),
            sort_active: AtomicUsize::new(0),
            expand_peak: AtomicU64::new(0),
            expand_current: AtomicU64::new(0),
            sort_peak: AtomicU64::new(0),
            sort_current: AtomicU64::new(0),
            expand_lifetimes: Mutex::new(BTreeMap::new()),
            sort_exclusive: Mutex::new(ActiveRssLifetime::default()),
            stop: AtomicBool::new(false),
        }
    }
}

#[derive(Default)]
struct ActiveRssLifetime {
    active: usize,
    before_bytes: u64,
    peak_bytes: u64,
    current_bytes: u64,
    after_bytes: u64,
}

impl ActiveRssLifetime {
    fn snapshot(&self) -> RssLifetimeSnapshot {
        RssLifetimeSnapshot {
            before_bytes: self.before_bytes,
            peak_bytes: self.peak_bytes,
            current_bytes: self.current_bytes,
            after_bytes: self.after_bytes,
        }
    }
}

/// Run one future with isolated, task-scoped query evidence.
pub async fn observe<F: std::future::Future>(future: F) -> (F::Output, DemandSnapshot) {
    let capture = Arc::new(QueryCapture::new());
    let sampler_capture = Arc::clone(&capture);
    let sampler = std::thread::spawn(move || sample_rss(&sampler_capture));
    let guard = SamplerGuard {
        capture: Arc::clone(&capture),
        sampler: Some(sampler),
    };
    let output = ACTIVE_CAPTURE.scope(Arc::clone(&capture), future).await;
    drop(guard);
    let mut snapshot = capture.snapshot.lock().expect("query capture lock").clone();
    snapshot.operator_rss = OperatorRssSnapshot {
        expand_peak_bytes: capture.expand_peak.load(Ordering::Acquire),
        expand_current_bytes: capture.expand_current.load(Ordering::Acquire),
        sort_peak_bytes: capture.sort_peak.load(Ordering::Acquire),
        sort_current_bytes: capture.sort_current.load(Ordering::Acquire),
        expand_by_hop: capture
            .expand_lifetimes
            .lock()
            .expect("expand RSS lifetime lock")
            .iter()
            .map(|(edge_var, lifetime)| (*edge_var, lifetime.snapshot()))
            .collect(),
        sort_exclusive: capture
            .sort_exclusive
            .lock()
            .expect("sort RSS lifetime lock")
            .snapshot(),
    };
    (output, snapshot)
}

struct SamplerGuard {
    capture: Arc<QueryCapture>,
    sampler: Option<std::thread::JoinHandle<()>>,
}

impl Drop for SamplerGuard {
    fn drop(&mut self) {
        self.capture.stop.store(true, Ordering::Release);
        if let Some(sampler) = self.sampler.take() {
            let _ = sampler.join();
        }
    }
}

#[cfg(test)]
static ACTIVE_SAMPLERS: AtomicUsize = AtomicUsize::new(0);

fn with_capture(update: impl FnOnce(&QueryCapture)) {
    let _ = ACTIVE_CAPTURE.try_with(|capture| update(capture));
}

/// Explicit query-capture context for physical operators whose streams may be
/// polled by DataFusion tasks that do not inherit Tokio task locals.
#[derive(Clone)]
pub(crate) struct CaptureHandle(Arc<QueryCapture>);

pub(crate) fn capture_handle() -> Option<CaptureHandle> {
    ACTIVE_CAPTURE
        .try_with(|capture| CaptureHandle(Arc::clone(capture)))
        .ok()
}

fn with_handle(handle: Option<&CaptureHandle>, update: impl FnOnce(&QueryCapture)) {
    if let Some(handle) = handle {
        update(&handle.0);
    } else {
        with_capture(update);
    }
}

pub(crate) fn capture_enabled() -> bool {
    ACTIVE_CAPTURE.try_with(|_| ()).is_ok()
}

fn current_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let kib = status
        .lines()
        .find(|line| line.starts_with("VmRSS:"))?
        .split_whitespace()
        .nth(1)?
        .parse::<u64>()
        .ok()?;
    Some(kib.saturating_mul(1024))
}

fn sample_rss(capture: &QueryCapture) {
    #[cfg(test)]
    ACTIVE_SAMPLERS.fetch_add(1, Ordering::AcqRel);
    while !capture.stop.load(Ordering::Acquire) {
        if let Some(rss) = current_rss_bytes() {
            if capture.expand_active.load(Ordering::Acquire) > 0 {
                capture.expand_current.store(rss, Ordering::Release);
                capture.expand_peak.fetch_max(rss, Ordering::AcqRel);
                for lifetime in capture
                    .expand_lifetimes
                    .lock()
                    .expect("expand RSS lifetime lock")
                    .values_mut()
                    .filter(|lifetime| lifetime.active > 0)
                {
                    lifetime.current_bytes = rss;
                    lifetime.peak_bytes = lifetime.peak_bytes.max(rss);
                }
            }
            if capture.sort_active.load(Ordering::Acquire) > 0 {
                capture.sort_current.store(rss, Ordering::Release);
                capture.sort_peak.fetch_max(rss, Ordering::AcqRel);
                if capture.expand_active.load(Ordering::Acquire) == 0 {
                    let mut lifetime = capture
                        .sort_exclusive
                        .lock()
                        .expect("sort RSS lifetime lock");
                    lifetime.current_bytes = rss;
                    lifetime.peak_bytes = lifetime.peak_bytes.max(rss);
                }
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    #[cfg(test)]
    ACTIVE_SAMPLERS.fetch_sub(1, Ordering::AcqRel);
}

pub(crate) struct OperatorActivity {
    kind: OperatorKind,
    capture: Option<Arc<QueryCapture>>,
}
enum OperatorKind {
    Expand(u32),
    Sort,
}
impl OperatorActivity {
    pub(crate) fn expand(edge_var: u32) -> Self {
        Self::new(OperatorKind::Expand(edge_var))
    }
    pub(crate) fn expand_with_capture(edge_var: u32, capture: Option<CaptureHandle>) -> Self {
        Self::new_with_capture(OperatorKind::Expand(edge_var), capture)
    }
    fn sort() -> Self {
        Self::new(OperatorKind::Sort)
    }
    fn new(kind: OperatorKind) -> Self {
        Self::new_with_capture(kind, capture_handle())
    }
    fn new_with_capture(kind: OperatorKind, capture: Option<CaptureHandle>) -> Self {
        let capture = capture.map(|handle| handle.0);
        if let Some(capture) = &capture {
            match kind {
                OperatorKind::Expand(_) => &capture.expand_active,
                OperatorKind::Sort => &capture.sort_active,
            }
            .fetch_add(1, Ordering::AcqRel);
            let rss = current_rss_bytes().unwrap_or(0);
            match kind {
                OperatorKind::Expand(edge_var) => {
                    let mut lifetimes = capture
                        .expand_lifetimes
                        .lock()
                        .expect("expand RSS lifetime lock");
                    let lifetime = lifetimes.entry(edge_var).or_default();
                    if lifetime.active == 0 {
                        lifetime.before_bytes = rss;
                    }
                    lifetime.active += 1;
                }
                OperatorKind::Sort => {
                    let mut lifetime = capture
                        .sort_exclusive
                        .lock()
                        .expect("sort RSS lifetime lock");
                    if lifetime.active == 0 {
                        lifetime.before_bytes = rss;
                    }
                    lifetime.active += 1;
                }
            }
        }
        Self { kind, capture }
    }
}

pub(crate) fn sort_activity(plan: &Arc<dyn ExecutionPlan>) -> Option<OperatorActivity> {
    fn contains(plan: &Arc<dyn ExecutionPlan>) -> bool {
        plan.is::<SortExec>() || plan.children().into_iter().any(contains)
    }
    contains(plan).then(OperatorActivity::sort)
}
impl Drop for OperatorActivity {
    fn drop(&mut self) {
        if let Some(capture) = &self.capture {
            match self.kind {
                OperatorKind::Expand(_) => &capture.expand_active,
                OperatorKind::Sort => &capture.sort_active,
            }
            .fetch_sub(1, Ordering::AcqRel);
            let rss = current_rss_bytes().unwrap_or(0);
            match self.kind {
                OperatorKind::Expand(edge_var) => {
                    let mut lifetimes = capture
                        .expand_lifetimes
                        .lock()
                        .expect("expand RSS lifetime lock");
                    let lifetime = lifetimes.entry(edge_var).or_default();
                    lifetime.active = lifetime.active.saturating_sub(1);
                    if lifetime.active == 0 {
                        lifetime.after_bytes = rss;
                    }
                }
                OperatorKind::Sort => {
                    let mut lifetime = capture
                        .sort_exclusive
                        .lock()
                        .expect("sort RSS lifetime lock");
                    lifetime.active = lifetime.active.saturating_sub(1);
                    if lifetime.active == 0 {
                        lifetime.after_bytes = rss;
                    }
                }
            }
        }
    }
}

pub(crate) fn record_memory_before(bytes: usize) {
    with_capture(|capture| {
        capture
            .snapshot
            .lock()
            .expect("query capture lock")
            .memory_reserved_before = bytes as u64;
    });
}

/// Capture metrics only after collection has dropped every operator stream.
pub(crate) fn record_plan_after(
    plan: &Arc<dyn ExecutionPlan>,
    memory_reserved_after: usize,
    returned_batch_bytes: usize,
) {
    fn value(metrics: &datafusion::physical_plan::metrics::MetricsSet, name: &str) -> u64 {
        metrics
            .sum(|metric| metric.value().name() == name)
            .map_or(0, |metric| metric.as_usize() as u64)
    }
    fn visit(plan: &Arc<dyn ExecutionPlan>, sorts: &mut Vec<SortSnapshot>) {
        if plan.is::<SortExec>() {
            let metrics = plan.metrics().unwrap_or_default();
            sorts.push(SortSnapshot {
                ordinal: sorts.len(),
                fetch: plan.fetch(),
                output_rows: metrics.output_rows().map_or(0, |rows| rows as u64),
                output_batches: value(&metrics, "output_batches"),
                spill_count: metrics.spill_count().map_or(0, |count| count as u64),
                spilled_bytes: metrics.spilled_bytes().map_or(0, |bytes| bytes as u64),
                memory_used_after: value(&metrics, "mem_used"),
            });
        }
        for child in plan.children() {
            visit(child, sorts);
        }
    }

    with_capture(|capture| {
        let mut snapshot = capture.snapshot.lock().expect("query capture lock");
        snapshot.sorts.clear();
        visit(plan, &mut snapshot.sorts);
        snapshot.memory_reserved_after = memory_reserved_after as u64;
        snapshot.returned_batch_bytes = returned_batch_bytes as u64;
    });
}

fn with_hop(edge_var: u32, update: impl FnOnce(&mut HopSnapshot)) {
    with_hop_handle(None, edge_var, update);
}

fn with_hop_handle(
    handle: Option<&CaptureHandle>,
    edge_var: u32,
    update: impl FnOnce(&mut HopSnapshot),
) {
    with_handle(handle, |capture| {
        update(
            capture
                .snapshot
                .lock()
                .expect("query capture lock")
                .hops
                .entry(edge_var)
                .or_default(),
        );
    });
}

pub(crate) fn record_input(edge_var: u32, rows: usize) {
    record_input_with_capture(None, edge_var, rows);
}

pub(crate) fn record_input_with_capture(
    capture: Option<&CaptureHandle>,
    edge_var: u32,
    rows: usize,
) {
    with_hop_handle(capture, edge_var, |hop| {
        hop.input_batches += 1;
        hop.input_rows += rows as u64;
    });
}

pub(crate) fn record_candidates_with_capture(
    capture: Option<&CaptureHandle>,
    edge_var: u32,
    rows: usize,
) {
    with_hop_handle(capture, edge_var, |hop| {
        hop.candidates_generated += rows as u64
    });
}

pub(crate) fn record_emitted_with_capture(
    capture: Option<&CaptureHandle>,
    edge_var: u32,
    rows: usize,
) {
    with_hop_handle(capture, edge_var, |hop| hop.rows_emitted += rows as u64);
}

fn record_filter(ordinal: usize, uniqueness: bool, input: bool, rows: usize) {
    with_capture(|capture| {
        let mut snapshot = capture.snapshot.lock().expect("query capture lock");
        let filter = snapshot.filters.entry(ordinal).or_insert(FilterSnapshot {
            ordinal,
            relationship_uniqueness: uniqueness,
            ..FilterSnapshot::default()
        });
        if input {
            filter.input_rows += rows as u64;
        } else {
            filter.output_rows += rows as u64;
        }
    });
}

/// Shared state attached to every fixed hop in one bounded physical plan.
pub(crate) struct QueryDemand {
    cancelled: AtomicBool,
    in_flight_reads: AtomicUsize,
    max_in_flight_reads: AtomicUsize,
    produced_rows: AtomicUsize,
    quiescent_waker: AtomicWaker,
    capture: Option<CaptureHandle>,
}

impl QueryDemand {
    fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            in_flight_reads: AtomicUsize::new(0),
            max_in_flight_reads: AtomicUsize::new(0),
            produced_rows: AtomicUsize::new(0),
            quiescent_waker: AtomicWaker::new(),
            capture: capture_handle(),
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn cancel(&self) {
        if !self.cancelled.swap(true, Ordering::AcqRel) && self.capture.is_some() {
            with_handle(self.capture.as_ref(), |capture| {
                capture
                    .snapshot
                    .lock()
                    .expect("query capture lock")
                    .cancellations += 1;
            });
        }
    }

    fn observe_output(&self, rows: usize, cancel_after: usize) -> bool {
        let before = self.produced_rows.fetch_add(rows, Ordering::AcqRel);
        before.saturating_add(rows) >= cancel_after
    }

    pub(crate) fn begin_read(self: &Arc<Self>, edge_var: u32) -> Option<ReadPermit> {
        if self.is_cancelled() {
            with_hop_handle(self.capture.as_ref(), edge_var, |hop| {
                hop.reads_after_cancel += 1
            });
            return None;
        }
        let current = self.in_flight_reads.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_in_flight_reads
            .fetch_max(current, Ordering::AcqRel);
        if self.capture.is_some() {
            with_handle(self.capture.as_ref(), |capture| {
                let mut snapshot = capture.snapshot.lock().expect("query capture lock");
                snapshot.max_in_flight_reads = snapshot.max_in_flight_reads.max(current as u64);
            });
        }
        if self.is_cancelled() {
            self.finish_read();
            with_hop_handle(self.capture.as_ref(), edge_var, |hop| {
                hop.reads_after_cancel += 1
            });
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
    capture: Option<CaptureHandle>,
}

impl HopReadObserver {
    pub(crate) fn with_capture(edge_var: u32, capture: Option<CaptureHandle>) -> Self {
        Self { edge_var, capture }
    }
}

impl graphforge_storage::io_stats::FilteredReadObserver for HopReadObserver {
    fn read_started(&self, table: graphforge_storage::io_stats::FilteredReadTable) {
        with_hop_handle(self.capture.as_ref(), self.edge_var, |hop| match table {
            graphforge_storage::io_stats::FilteredReadTable::Edge => hop.edge_reads_started += 1,
            graphforge_storage::io_stats::FilteredReadTable::Node => hop.node_reads_started += 1,
        });
    }

    fn rows_scanned(&self, table: graphforge_storage::io_stats::FilteredReadTable, rows: u64) {
        with_hop_handle(self.capture.as_ref(), self.edge_var, |hop| match table {
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
        with_hop_handle(self.capture.as_ref(), self.edge_var, |hop| match table {
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
        with_hop_handle(self.capture.as_ref(), self.edge_var, |hop| match table {
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
        with_hop_handle(self.capture.as_ref(), self.edge_var, |hop| {
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
    if let Some(limit) = plan.downcast_ref::<GlobalLimitExec>() {
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
    plan.is::<GlobalLimitExec>()
        || plan.is::<LocalLimitExec>()
        || plan.is::<CoalescePartitionsExec>()
        || plan.is::<FilterExec>()
        || plan.is::<ProjectionExec>()
        || plan.is::<RepartitionExec>()
        || plan.is::<ExpandExec>()
}

/// Whether a fixed hop is reachable without crossing a semantic boundary.
/// This is intentionally stricter than a whole-tree search: an exchange above
/// a sort/join/aggregate must not be treated as part of the bounded pipeline.
fn contains_demand_expand(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if plan.is::<ExpandExec>() {
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
    if let Some(repartition) = plan.downcast_ref::<RepartitionExec>()
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

    if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        let ordinal = *filter_ordinal;
        *filter_ordinal += 1;
        let uniqueness = filter
            .predicate()
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
    if let Some(expand) = rebuilt.downcast_ref::<ExpandExec>() {
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
    use std::sync::{Arc, LazyLock};
    use std::task::Context;

    use arrow::datatypes::Schema;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::limit::GlobalLimitExec;
    use datafusion::physical_plan::stream::EmptyRecordBatchStream;

    use super::*;

    static OBSERVATION_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    #[tokio::test]
    async fn capture_is_task_scoped_for_overlapping_nested_error_and_unobserved_work() {
        let _guard = OBSERVATION_TEST_LOCK.lock().unwrap();
        record_input(99, 1);
        let left = observe(async {
            record_memory_before(17);
            record_input(1, 2);
            let (_, nested) = observe(async { record_input(2, 3) }).await;
            assert_eq!(nested.hops[&2].input_rows, 3);
            let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
            record_plan_after(&plan, 0, 0);
            Err::<(), _>("typed failure")
        });
        let right = observe(async { record_input(7, 5) });
        let ((left_result, left), (_, right)) = tokio::join!(left, right);
        assert_eq!(left_result, Err("typed failure"));
        assert_eq!(left.hops.len(), 1);
        assert_eq!(left.hops[&1].input_rows, 2);
        assert_eq!(
            (left.memory_reserved_before, left.memory_reserved_after),
            (17, 0)
        );
        assert_eq!(right.hops.len(), 1);
        assert_eq!(right.hops[&7].input_rows, 5);
        assert!(!left.hops.contains_key(&99));
    }

    #[tokio::test]
    async fn explicit_capture_handle_survives_spawned_operator_task() {
        let _guard = OBSERVATION_TEST_LOCK.lock().unwrap();
        let (_, snapshot) = observe(async {
            let capture = capture_handle().expect("active query capture");
            tokio::spawn(async move {
                record_input_with_capture(Some(&capture), 41, 3);
                record_candidates_with_capture(Some(&capture), 41, 5);
                record_emitted_with_capture(Some(&capture), 41, 5);
                let _activity = OperatorActivity::expand_with_capture(41, Some(capture));
            })
            .await
            .unwrap();
        })
        .await;

        assert_eq!(snapshot.hops[&41].input_rows, 3);
        assert_eq!(snapshot.hops[&41].candidates_generated, 5);
        assert_eq!(snapshot.hops[&41].rows_emitted, 5);
        assert!(snapshot.operator_rss.expand_by_hop.contains_key(&41));
    }

    #[tokio::test]
    async fn sampler_is_reaped_when_observation_is_aborted_or_panics() {
        let _guard = OBSERVATION_TEST_LOCK.lock().unwrap();
        let aborted = tokio::spawn(async {
            observe(std::future::pending::<()>()).await;
        });
        tokio::task::yield_now().await;
        aborted.abort();
        assert!(aborted.await.unwrap_err().is_cancelled());
        assert_eq!(ACTIVE_SAMPLERS.load(Ordering::Acquire), 0);

        let panicked = tokio::spawn(async {
            observe(async { panic!("observed future panic") }).await;
        });
        assert!(panicked.await.unwrap_err().is_panic());
        assert_eq!(ACTIVE_SAMPLERS.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn rss_lifetimes_separate_each_expand_from_sort_only_work() {
        let _guard = OBSERVATION_TEST_LOCK.lock().unwrap();
        let (_, snapshot) = observe(async {
            let sort = OperatorActivity::sort();
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            {
                let _first = OperatorActivity::expand(11);
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            {
                let _second = OperatorActivity::expand(22);
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            drop(sort);
        })
        .await;

        assert_eq!(
            snapshot
                .operator_rss
                .expand_by_hop
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            [11, 22]
        );
        for lifetime in snapshot.operator_rss.expand_by_hop.values() {
            assert!(lifetime.before_bytes > 0 || !cfg!(target_os = "linux"));
            assert!(lifetime.after_bytes > 0 || !cfg!(target_os = "linux"));
            assert!(lifetime.peak_bytes >= lifetime.current_bytes);
        }
        let sort = &snapshot.operator_rss.sort_exclusive;
        assert!(sort.before_bytes > 0 || !cfg!(target_os = "linux"));
        assert!(sort.after_bytes > 0 || !cfg!(target_os = "linux"));
        assert!(sort.peak_bytes >= sort.current_bytes);
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
        let probe: Arc<dyn ExecutionPlan> =
            Arc::new(ProbeExec::new(Arc::clone(&child), 3, true, true));
        assert_eq!(probe.name(), "DemandProbeExec");
        assert!(probe.is::<ProbeExec>());
        assert!(format!("{probe:?}").contains("ordinal: 3"));
        assert!(
            format!(
                "{}",
                datafusion::physical_plan::displayable(probe.as_ref()).one_line()
            )
            .contains("side=input")
        );
        assert_eq!(probe.children().len(), 1);
        assert_eq!(probe.schema(), schema);

        let demand = Arc::new(QueryDemand::new());
        let guard: Arc<dyn ExecutionPlan> =
            Arc::new(DemandGuardExec::new(child, Arc::clone(&demand), 2));
        assert!(guard.is::<DemandGuardExec>());
        assert!(format!("{guard:?}").contains("cancel_after: 2"));
        assert!(
            format!(
                "{}",
                datafusion::physical_plan::displayable(guard.as_ref()).one_line()
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
