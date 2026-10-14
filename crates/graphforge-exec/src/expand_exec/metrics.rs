//! DataFusion metrics of the expansion operators, shown by `EXPLAIN ANALYZE`.
//!
//! Each operator owns one [`ExpandMetrics`] set. An execution registers the
//! counters of its partition once ([`ExpandMetrics::partition`]) and carries
//! the handle in its configuration, so the BFS task and the single-hop stream
//! count where the work happens. The counts come from the same quantities the
//! diagnostic capture in `demand` records (input rows, adjacency entries
//! examined, rows a storage read returned) but live in the operator's metric
//! set; they never write to the capture or to the storage I/O counters, so
//! nothing is counted twice.
//!
//! * `output_rows`, `output_bytes`, `output_batches`: the batches handed on.
//! * `elapsed_compute`: time spent expanding, reading storage and building
//!   batches; time waiting for the input or for the consumer is excluded.
//! * `input_rows`: rows received from the input.
//! * `adjacency_entries_examined`: adjacency entries the traversal looked at,
//!   before any edge filter, uniqueness or destination check.
//! * `edge_rows_read`: relationship rows a storage read returned.
//! * `destination_rows_read`: destination node rows a storage read (or the
//!   identity authority) returned.

use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, RecordOutput,
    ScopedTimerGuard,
};

use super::RecordBatch;

/// The metric set of one expansion operator, shared by every copy of it.
#[derive(Debug, Clone, Default)]
pub(crate) struct ExpandMetrics {
    set: ExecutionPlanMetricsSet,
}

impl ExpandMetrics {
    /// What `ExecutionPlan::metrics` returns.
    pub(crate) fn snapshot(&self) -> MetricsSet {
        self.set.clone_inner()
    }

    /// Register the counters of one partition's execution.
    pub(crate) fn partition(&self, partition: usize) -> PartitionMetrics {
        let counter =
            |name: &'static str| MetricBuilder::new(&self.set).counter(name, partition);
        PartitionMetrics {
            baseline: BaselineMetrics::new(&self.set, partition),
            input_rows: counter("input_rows"),
            adjacency_entries: counter("adjacency_entries_examined"),
            edge_rows: counter("edge_rows_read"),
            destination_rows: counter("destination_rows_read"),
        }
    }
}

/// The counters of one partition's execution of an expansion operator.
#[derive(Debug, Clone)]
pub(crate) struct PartitionMetrics {
    baseline: BaselineMetrics,
    input_rows: Count,
    adjacency_entries: Count,
    edge_rows: Count,
    destination_rows: Count,
}

impl PartitionMetrics {
    /// Time the guard's scope as `elapsed_compute`. Hold it only across work
    /// that does not wait on the input or the consumer.
    pub(crate) fn compute(&self) -> ScopedTimerGuard<'_> {
        self.baseline.elapsed_compute().timer()
    }

    /// Count a batch handed to the consumer as output and return it.
    pub(crate) fn emitted(&self, batch: RecordBatch) -> RecordBatch {
        batch.record_output(&self.baseline)
    }

    pub(crate) fn input_rows(&self, rows: usize) {
        self.input_rows.add(rows);
    }

    pub(crate) fn adjacency_entries(&self, entries: usize) {
        self.adjacency_entries.add(entries);
    }

    pub(crate) fn edge_rows(&self, rows: usize) {
        self.edge_rows.add(rows);
    }

    pub(crate) fn destination_rows(&self, rows: usize) {
        self.destination_rows.add(rows);
    }
}
