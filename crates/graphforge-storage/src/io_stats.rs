//! Requested operation-owned I/O counters for storage reads and staged rewrite commits.
//! The read counters cover [`read_edges`](crate::catalog::read_edges),
//! [`read_edges_filtered`](crate::catalog::read_edges_filtered), and
//! [`read_nodes`](crate::catalog::read_nodes).
//!
//! # Why
//! These prove the adjacency-IO T1 criterion (#767): with the adjacency index present,
//! variable-length traversal must not scan the full edge file — it issues only
//! an `edge_id`-filtered read whose materialized row count is proportional to
//! the traversed neighborhood, independent of the total edge count. A scan
//! over the index path can then assert `edge_full_reads == 0`, while the
//! scan-build baseline shows `edge_full_rows >= total_edges`.
//!
//! # Semantics
//! - A **full read** is one decode of a whole file: `read_edges` / `read_nodes`,
//!   plus the [`read_edges_filtered`](crate::catalog::read_edges_filtered)
//!   *fallback* (a requested id set covering more than half the file reads it
//!   whole, then trims in memory — it is a full scan and is counted as one, so
//!   it cannot hide behind the filtered API).
//! - A **filtered read** is the predicate-pushdown path of
//!   [`read_edges_filtered`](crate::catalog::read_edges_filtered); its row count
//!   is the rows actually materialized after row-group and row-filter pruning.
//! - `rows` count rows actually returned to the caller. A missing or empty file
//!   still counts as one read of zero rows (the reader was invoked).
//! - A **rewrite commit** is one successful, non-empty
//!   [`RewriteBatch`](crate::RewriteBatch) commit, regardless of how many files
//!   were staged in that batch.
//!
//! # Collection
//! Ordinary operations do not update these counters. Install [`CaptureScope`]
//! before the measured work; workers attach its lifecycle context. [`snapshot`]
//! returns `None` when no collector is active. [`reset`] clears only an existing
//! capture and never enables collection for later unrelated operations.

use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(test)]
static TEST_MEASUREMENT_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Legacy serialization for tests sharing storage fixtures. Each measurement
/// must separately install an operation-owned capture.
#[cfg(test)]
pub(crate) fn test_measurement_guard() -> std::sync::MutexGuard<'static, ()> {
    TEST_MEASUREMENT_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Table family involved in one filtered topology read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum FilteredReadTable {
    /// Relationship topology.
    Edge,
    /// Node topology.
    Node,
}

/// Storage strategy used for one filtered topology read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum FilteredReadStrategy {
    /// Exact row ordinals derived from a proven dense node-id layout.
    DenseRowSelection,
    /// Conservative row-group pruning plus a Parquet row predicate.
    RowGroupPredicate,
    /// More than half the file was requested, so the whole file was read.
    FullFallback,
}

/// Aggregate-only pruning work for one physical filtered-read attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct FilteredReadPruning {
    /// Strategy used by the attempt.
    pub strategy: FilteredReadStrategy,
    /// Row groups whose metadata was considered.
    pub row_groups_considered: u64,
    /// Row groups retained for decoding.
    pub row_groups_selected: u64,
    /// Key-column pages whose metadata was considered.
    pub pages_considered: u64,
    /// Key-column pages containing at least one selected row.
    pub pages_selected: u64,
    /// Exact row ordinals selected before the membership guard.
    pub exact_rows_selected: u64,
    /// Dense selection was unavailable because its metadata contract failed.
    pub metadata_fallbacks: u64,
    /// Dense output validation failed and triggered a conservative retry.
    pub validation_fallbacks: u64,
}

/// Optional observer for attributing filtered-read work to a physical operator.
///
/// The normal storage API installs no observer. Traversal diagnostics use this
/// hook to distinguish concurrent hops without recording requested ids, paths,
/// or graph contents.
#[doc(hidden)]
pub trait FilteredReadObserver: Send + Sync {
    /// A physical Parquet read is about to be opened.
    fn read_started(&self, table: FilteredReadTable);

    /// Rows evaluated by the Parquet predicate/page-index path.
    fn rows_scanned(&self, table: FilteredReadTable, rows: u64);

    /// A physical read completed, with its returned row count and whether it
    /// used the full-read fallback.
    fn read_completed(&self, table: FilteredReadTable, rows: u64, full: bool);

    /// A started physical read failed before completion.
    fn read_failed(&self, table: FilteredReadTable);

    /// Aggregate pruning work for a completed physical attempt.
    fn pruning(&self, _table: FilteredReadTable, _pruning: FilteredReadPruning) {}
}

/// Collect optional read/rewrite statistics with an operation-owned lifecycle capture.
pub use crate::lifecycle_io::CaptureScope;

#[derive(Debug, Default)]
pub(crate) struct Counters {
    edge_full_reads: AtomicU64,
    edge_full_rows: AtomicU64,
    edge_filtered_reads: AtomicU64,
    edge_filtered_rows: AtomicU64,
    node_full_reads: AtomicU64,
    node_full_rows: AtomicU64,
    node_filtered_reads: AtomicU64,
    node_filtered_rows: AtomicU64,
    edge_scanned_rows: AtomicU64,
    node_scanned_rows: AtomicU64,
    node_dense_row_selection_reads: AtomicU64,
    node_row_group_predicate_reads: AtomicU64,
    node_row_groups_considered: AtomicU64,
    node_row_groups_selected: AtomicU64,
    node_pages_considered: AtomicU64,
    node_pages_selected: AtomicU64,
    node_exact_rows_selected: AtomicU64,
    node_metadata_fallbacks: AtomicU64,
    node_validation_fallbacks: AtomicU64,
    rewrite_commits: AtomicU64,
    topology_rewrite_existing_rows: AtomicU64,
    topology_rewrite_new_rows: AtomicU64,
    topology_rewrite_output_rows: AtomicU64,
    topology_rewrite_peak_batch_rows: AtomicU64,
    uuid_files_opened: AtomicU64,
    uuid_files_synced: AtomicU64,
    relationship_merge_topology_rows: AtomicU64,
    relationship_merge_candidate_rows: AtomicU64,
    relationship_merge_property_rows: AtomicU64,
}

/// A point-in-time copy of requested I/O counters. Difference two
/// snapshots — or [`reset`] then [`snapshot`] — to attribute work to a region.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IoSnapshot {
    /// Full edge-file reads: `read_edges` plus the filtered-read fallback.
    pub edge_full_reads: u64,
    /// Rows returned by those full edge reads.
    pub edge_full_rows: u64,
    /// `edge_id`-filtered edge reads that took the predicate-pushdown path.
    pub edge_filtered_reads: u64,
    /// Rows materialized by those filtered reads (post-pruning).
    pub edge_filtered_rows: u64,
    /// Full node-file reads: `read_nodes` plus the filtered-read fallback.
    pub node_full_reads: u64,
    /// Rows returned by those full node reads.
    pub node_full_rows: u64,
    /// `node_id`-filtered node reads that took the predicate-pushdown path
    /// (`read_nodes_filtered`, #838).
    pub node_filtered_reads: u64,
    /// Rows materialized by those filtered node reads (post-pruning).
    pub node_filtered_rows: u64,
    /// Edge rows the pushdown predicate actually evaluated — i.e. rows in the
    /// data pages the page index did **not** skip. The decode-cost proxy: for a
    /// clustered (localized) id set this is a few pages regardless of total file
    /// size; for a scattered set it approaches the whole file. (#838)
    pub edge_scanned_rows: u64,
    /// Node rows the pushdown predicate evaluated (pages not page-index-skipped).
    pub node_scanned_rows: u64,
    /// Node reads that used exact dense row selection.
    pub node_dense_row_selection_reads: u64,
    /// Node reads that used conservative row-group and predicate pruning.
    pub node_row_group_predicate_reads: u64,
    /// Node row groups whose metadata was considered for pruning.
    pub node_row_groups_considered: u64,
    /// Node row groups retained for decoding.
    pub node_row_groups_selected: u64,
    /// Node-id pages considered by exact dense selection.
    pub node_pages_considered: u64,
    /// Node-id pages containing selected rows.
    pub node_pages_selected: u64,
    /// Exact node row ordinals selected before the membership guard.
    pub node_exact_rows_selected: u64,
    /// Dense selection attempts rejected by the metadata contract.
    pub node_metadata_fallbacks: u64,
    /// Dense selection attempts rejected by post-read validation.
    pub node_validation_fallbacks: u64,
    /// Successful non-empty [`RewriteBatch`](crate::RewriteBatch) commits.
    /// This counts persistence cycles, not the number of files in a batch.
    pub rewrite_commits: u64,
    /// Existing topology rows decoded and copied by append-style rewrites.
    pub topology_rewrite_existing_rows: u64,
    /// New topology rows supplied to append-style rewrites.
    pub topology_rewrite_new_rows: u64,
    /// Total topology rows materialized as rewrite outputs.
    pub topology_rewrite_output_rows: u64,
    /// Largest decoded or newly supplied batch held by a topology rewrite.
    pub topology_rewrite_peak_batch_rows: u64,
    /// Successful physical file opens/creates on the UUID publication path.
    pub uuid_files_opened: u64,
    /// Successful physical file durability syncs on the UUID publication path.
    pub uuid_files_synced: u64,
    /// Relationship topology rows inspected while building MERGE's clause-local endpoint index.
    pub relationship_merge_topology_rows: u64,
    /// Indexed endpoint candidates inspected while resolving MERGE input rows.
    pub relationship_merge_candidate_rows: u64,
    /// Authenticated property rows decoded for relationship MERGE candidates.
    pub relationship_merge_property_rows: u64,
}

/// Capture requested counters, or report an unavailable observation.
#[must_use]
pub fn snapshot() -> Option<IoSnapshot> {
    crate::lifecycle_io::with_io_stats(|counters| IoSnapshot {
        edge_full_reads: counters.edge_full_reads.load(Ordering::Relaxed),
        edge_full_rows: counters.edge_full_rows.load(Ordering::Relaxed),
        edge_filtered_reads: counters.edge_filtered_reads.load(Ordering::Relaxed),
        edge_filtered_rows: counters.edge_filtered_rows.load(Ordering::Relaxed),
        node_full_reads: counters.node_full_reads.load(Ordering::Relaxed),
        node_full_rows: counters.node_full_rows.load(Ordering::Relaxed),
        node_filtered_reads: counters.node_filtered_reads.load(Ordering::Relaxed),
        node_filtered_rows: counters.node_filtered_rows.load(Ordering::Relaxed),
        edge_scanned_rows: counters.edge_scanned_rows.load(Ordering::Relaxed),
        node_scanned_rows: counters.node_scanned_rows.load(Ordering::Relaxed),
        node_dense_row_selection_reads: counters
            .node_dense_row_selection_reads
            .load(Ordering::Relaxed),
        node_row_group_predicate_reads: counters
            .node_row_group_predicate_reads
            .load(Ordering::Relaxed),
        node_row_groups_considered: counters.node_row_groups_considered.load(Ordering::Relaxed),
        node_row_groups_selected: counters.node_row_groups_selected.load(Ordering::Relaxed),
        node_pages_considered: counters.node_pages_considered.load(Ordering::Relaxed),
        node_pages_selected: counters.node_pages_selected.load(Ordering::Relaxed),
        node_exact_rows_selected: counters.node_exact_rows_selected.load(Ordering::Relaxed),
        node_metadata_fallbacks: counters.node_metadata_fallbacks.load(Ordering::Relaxed),
        node_validation_fallbacks: counters.node_validation_fallbacks.load(Ordering::Relaxed),
        rewrite_commits: counters.rewrite_commits.load(Ordering::Relaxed),
        topology_rewrite_existing_rows: counters
            .topology_rewrite_existing_rows
            .load(Ordering::Relaxed),
        topology_rewrite_new_rows: counters.topology_rewrite_new_rows.load(Ordering::Relaxed),
        topology_rewrite_output_rows: counters
            .topology_rewrite_output_rows
            .load(Ordering::Relaxed),
        topology_rewrite_peak_batch_rows: counters
            .topology_rewrite_peak_batch_rows
            .load(Ordering::Relaxed),
        uuid_files_opened: counters.uuid_files_opened.load(Ordering::Relaxed),
        uuid_files_synced: counters.uuid_files_synced.load(Ordering::Relaxed),
        relationship_merge_topology_rows: counters
            .relationship_merge_topology_rows
            .load(Ordering::Relaxed),
        relationship_merge_candidate_rows: counters
            .relationship_merge_candidate_rows
            .load(Ordering::Relaxed),
        relationship_merge_property_rows: counters
            .relationship_merge_property_rows
            .load(Ordering::Relaxed),
    })
}

/// Reset the current capture; an inactive caller remains inactive.
pub fn reset() {
    let _ = crate::lifecycle_io::with_io_stats(|counters| {
        counters.edge_full_reads.store(0, Ordering::Relaxed);
        counters.edge_full_rows.store(0, Ordering::Relaxed);
        counters.edge_filtered_reads.store(0, Ordering::Relaxed);
        counters.edge_filtered_rows.store(0, Ordering::Relaxed);
        counters.node_full_reads.store(0, Ordering::Relaxed);
        counters.node_full_rows.store(0, Ordering::Relaxed);
        counters.node_filtered_reads.store(0, Ordering::Relaxed);
        counters.node_filtered_rows.store(0, Ordering::Relaxed);
        counters.edge_scanned_rows.store(0, Ordering::Relaxed);
        counters.node_scanned_rows.store(0, Ordering::Relaxed);
        counters
            .node_dense_row_selection_reads
            .store(0, Ordering::Relaxed);
        counters
            .node_row_group_predicate_reads
            .store(0, Ordering::Relaxed);
        counters
            .node_row_groups_considered
            .store(0, Ordering::Relaxed);
        counters
            .node_row_groups_selected
            .store(0, Ordering::Relaxed);
        counters.node_pages_considered.store(0, Ordering::Relaxed);
        counters.node_pages_selected.store(0, Ordering::Relaxed);
        counters
            .node_exact_rows_selected
            .store(0, Ordering::Relaxed);
        counters.node_metadata_fallbacks.store(0, Ordering::Relaxed);
        counters
            .node_validation_fallbacks
            .store(0, Ordering::Relaxed);
        counters.rewrite_commits.store(0, Ordering::Relaxed);
        counters
            .topology_rewrite_existing_rows
            .store(0, Ordering::Relaxed);
        counters
            .topology_rewrite_new_rows
            .store(0, Ordering::Relaxed);
        counters
            .topology_rewrite_output_rows
            .store(0, Ordering::Relaxed);
        counters
            .topology_rewrite_peak_batch_rows
            .store(0, Ordering::Relaxed);
        counters.uuid_files_opened.store(0, Ordering::Relaxed);
        counters.uuid_files_synced.store(0, Ordering::Relaxed);
        counters
            .relationship_merge_topology_rows
            .store(0, Ordering::Relaxed);
        counters
            .relationship_merge_candidate_rows
            .store(0, Ordering::Relaxed);
        counters
            .relationship_merge_property_rows
            .store(0, Ordering::Relaxed);
    });
}

/// Record actual clause-local relationship MERGE work without graph identities.
#[doc(hidden)]
pub fn record_relationship_merge_work(topology_rows: u64, candidate_rows: u64, property_rows: u64) {
    let _ = crate::lifecycle_io::with_io_stats(|counters| {
        counters
            .relationship_merge_topology_rows
            .fetch_add(topology_rows, Ordering::Relaxed);
        counters
            .relationship_merge_candidate_rows
            .fetch_add(candidate_rows, Ordering::Relaxed);
        counters
            .relationship_merge_property_rows
            .fetch_add(property_rows, Ordering::Relaxed);
    });
}

pub(crate) fn record_uuid_file_open() {
    let _ = crate::lifecycle_io::with_io_stats(|counters| {
        counters.uuid_files_opened.fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn record_uuid_file_sync() {
    let _ = crate::lifecycle_io::with_io_stats(|counters| {
        counters.uuid_files_synced.fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn record_edge_full_read(rows: u64) {
    let _ = crate::lifecycle_io::with_io_stats(|counters| {
        counters.edge_full_reads.fetch_add(1, Ordering::Relaxed);
        counters.edge_full_rows.fetch_add(rows, Ordering::Relaxed);
    });
}

pub(crate) fn record_edge_filtered_read(rows: u64) {
    let _ = crate::lifecycle_io::with_io_stats(|counters| {
        counters.edge_filtered_reads.fetch_add(1, Ordering::Relaxed);
        counters
            .edge_filtered_rows
            .fetch_add(rows, Ordering::Relaxed);
    });
}

pub(crate) fn record_node_full_read(rows: u64) {
    let _ = crate::lifecycle_io::with_io_stats(|counters| {
        counters.node_full_reads.fetch_add(1, Ordering::Relaxed);
        counters.node_full_rows.fetch_add(rows, Ordering::Relaxed);
    });
}

pub(crate) fn record_node_filtered_read(rows: u64) {
    let _ = crate::lifecycle_io::with_io_stats(|counters| {
        counters.node_filtered_reads.fetch_add(1, Ordering::Relaxed);
        counters
            .node_filtered_rows
            .fetch_add(rows, Ordering::Relaxed);
    });
}

pub(crate) fn record_edge_scanned(rows: u64) {
    let _ = crate::lifecycle_io::with_io_stats(|counters| {
        counters
            .edge_scanned_rows
            .fetch_add(rows, Ordering::Relaxed);
    });
}

pub(crate) fn record_node_scanned(rows: u64) {
    let _ = crate::lifecycle_io::with_io_stats(|counters| {
        counters
            .node_scanned_rows
            .fetch_add(rows, Ordering::Relaxed);
    });
}

pub(crate) fn record_node_pruning(pruning: FilteredReadPruning) {
    let _ = crate::lifecycle_io::with_io_stats(|counters| {
        match pruning.strategy {
            FilteredReadStrategy::DenseRowSelection => {
                counters
                    .node_dense_row_selection_reads
                    .fetch_add(1, Ordering::Relaxed);
            }
            FilteredReadStrategy::RowGroupPredicate => {
                counters
                    .node_row_group_predicate_reads
                    .fetch_add(1, Ordering::Relaxed);
            }
            FilteredReadStrategy::FullFallback => {}
        }
        counters
            .node_row_groups_considered
            .fetch_add(pruning.row_groups_considered, Ordering::Relaxed);
        counters
            .node_row_groups_selected
            .fetch_add(pruning.row_groups_selected, Ordering::Relaxed);
        counters
            .node_pages_considered
            .fetch_add(pruning.pages_considered, Ordering::Relaxed);
        counters
            .node_pages_selected
            .fetch_add(pruning.pages_selected, Ordering::Relaxed);
        counters
            .node_exact_rows_selected
            .fetch_add(pruning.exact_rows_selected, Ordering::Relaxed);
        counters
            .node_metadata_fallbacks
            .fetch_add(pruning.metadata_fallbacks, Ordering::Relaxed);
        counters
            .node_validation_fallbacks
            .fetch_add(pruning.validation_fallbacks, Ordering::Relaxed);
    });
}

pub(crate) fn record_rewrite_commit() {
    let _ = crate::lifecycle_io::with_io_stats(|counters| {
        counters.rewrite_commits.fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn record_topology_rewrite(existing_rows: u64, new_rows: u64) {
    let _ = crate::lifecycle_io::with_io_stats(|counters| {
        counters
            .topology_rewrite_existing_rows
            .fetch_add(existing_rows, Ordering::Relaxed);
        counters
            .topology_rewrite_new_rows
            .fetch_add(new_rows, Ordering::Relaxed);
        counters
            .topology_rewrite_output_rows
            .fetch_add(existing_rows.saturating_add(new_rows), Ordering::Relaxed);
    });
}

pub(crate) fn record_topology_rewrite_batch(rows: u64) {
    let _ = crate::lifecycle_io::with_io_stats(|counters| {
        let mut prior = counters
            .topology_rewrite_peak_batch_rows
            .load(Ordering::Relaxed);
        while rows > prior {
            match counters
                .topology_rewrite_peak_batch_rows
                .compare_exchange_weak(prior, rows, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => prior = actual,
            }
        }
    });
}
