//! Process-global I/O counters for storage reads and staged rewrite commits.
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
//! # Caveats
//! Counters are process-global and aggregate across threads *and* queries
//! (`read_nodes` is also used by the writer/mutator). They are advisory
//! instrumentation, not per-query state. A test that asserts on them must
//! [`reset`] immediately before the measured operation and keep that section
//! single-threaded — the counters cannot attribute concurrent work. Each
//! increment is one relaxed atomic add, negligible against a Parquet decode.

use std::sync::atomic::{AtomicU64, Ordering};

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

static EDGE_FULL_READS: AtomicU64 = AtomicU64::new(0);
static EDGE_FULL_ROWS: AtomicU64 = AtomicU64::new(0);
static EDGE_FILTERED_READS: AtomicU64 = AtomicU64::new(0);
static EDGE_FILTERED_ROWS: AtomicU64 = AtomicU64::new(0);
static NODE_FULL_READS: AtomicU64 = AtomicU64::new(0);
static NODE_FULL_ROWS: AtomicU64 = AtomicU64::new(0);
static NODE_FILTERED_READS: AtomicU64 = AtomicU64::new(0);
static NODE_FILTERED_ROWS: AtomicU64 = AtomicU64::new(0);
static EDGE_SCANNED_ROWS: AtomicU64 = AtomicU64::new(0);
static NODE_SCANNED_ROWS: AtomicU64 = AtomicU64::new(0);
static NODE_DENSE_ROW_SELECTION_READS: AtomicU64 = AtomicU64::new(0);
static NODE_ROW_GROUP_PREDICATE_READS: AtomicU64 = AtomicU64::new(0);
static NODE_ROW_GROUPS_CONSIDERED: AtomicU64 = AtomicU64::new(0);
static NODE_ROW_GROUPS_SELECTED: AtomicU64 = AtomicU64::new(0);
static NODE_PAGES_CONSIDERED: AtomicU64 = AtomicU64::new(0);
static NODE_PAGES_SELECTED: AtomicU64 = AtomicU64::new(0);
static NODE_EXACT_ROWS_SELECTED: AtomicU64 = AtomicU64::new(0);
static NODE_METADATA_FALLBACKS: AtomicU64 = AtomicU64::new(0);
static NODE_VALIDATION_FALLBACKS: AtomicU64 = AtomicU64::new(0);
static REWRITE_COMMITS: AtomicU64 = AtomicU64::new(0);

/// A point-in-time copy of the process-global I/O counters. Difference two
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
}

/// Capture the current process-global counters.
#[must_use]
pub fn snapshot() -> IoSnapshot {
    IoSnapshot {
        edge_full_reads: EDGE_FULL_READS.load(Ordering::Relaxed),
        edge_full_rows: EDGE_FULL_ROWS.load(Ordering::Relaxed),
        edge_filtered_reads: EDGE_FILTERED_READS.load(Ordering::Relaxed),
        edge_filtered_rows: EDGE_FILTERED_ROWS.load(Ordering::Relaxed),
        node_full_reads: NODE_FULL_READS.load(Ordering::Relaxed),
        node_full_rows: NODE_FULL_ROWS.load(Ordering::Relaxed),
        node_filtered_reads: NODE_FILTERED_READS.load(Ordering::Relaxed),
        node_filtered_rows: NODE_FILTERED_ROWS.load(Ordering::Relaxed),
        edge_scanned_rows: EDGE_SCANNED_ROWS.load(Ordering::Relaxed),
        node_scanned_rows: NODE_SCANNED_ROWS.load(Ordering::Relaxed),
        node_dense_row_selection_reads: NODE_DENSE_ROW_SELECTION_READS.load(Ordering::Relaxed),
        node_row_group_predicate_reads: NODE_ROW_GROUP_PREDICATE_READS.load(Ordering::Relaxed),
        node_row_groups_considered: NODE_ROW_GROUPS_CONSIDERED.load(Ordering::Relaxed),
        node_row_groups_selected: NODE_ROW_GROUPS_SELECTED.load(Ordering::Relaxed),
        node_pages_considered: NODE_PAGES_CONSIDERED.load(Ordering::Relaxed),
        node_pages_selected: NODE_PAGES_SELECTED.load(Ordering::Relaxed),
        node_exact_rows_selected: NODE_EXACT_ROWS_SELECTED.load(Ordering::Relaxed),
        node_metadata_fallbacks: NODE_METADATA_FALLBACKS.load(Ordering::Relaxed),
        node_validation_fallbacks: NODE_VALIDATION_FALLBACKS.load(Ordering::Relaxed),
        rewrite_commits: REWRITE_COMMITS.load(Ordering::Relaxed),
    }
}

/// Reset every counter to zero. Call immediately before a measured operation.
pub fn reset() {
    for c in [
        &EDGE_FULL_READS,
        &EDGE_FULL_ROWS,
        &EDGE_FILTERED_READS,
        &EDGE_FILTERED_ROWS,
        &NODE_FULL_READS,
        &NODE_FULL_ROWS,
        &NODE_FILTERED_READS,
        &NODE_FILTERED_ROWS,
        &EDGE_SCANNED_ROWS,
        &NODE_SCANNED_ROWS,
        &NODE_DENSE_ROW_SELECTION_READS,
        &NODE_ROW_GROUP_PREDICATE_READS,
        &NODE_ROW_GROUPS_CONSIDERED,
        &NODE_ROW_GROUPS_SELECTED,
        &NODE_PAGES_CONSIDERED,
        &NODE_PAGES_SELECTED,
        &NODE_EXACT_ROWS_SELECTED,
        &NODE_METADATA_FALLBACKS,
        &NODE_VALIDATION_FALLBACKS,
        &REWRITE_COMMITS,
    ] {
        c.store(0, Ordering::Relaxed);
    }
}

pub(crate) fn record_edge_full_read(rows: u64) {
    EDGE_FULL_READS.fetch_add(1, Ordering::Relaxed);
    EDGE_FULL_ROWS.fetch_add(rows, Ordering::Relaxed);
}

pub(crate) fn record_edge_filtered_read(rows: u64) {
    EDGE_FILTERED_READS.fetch_add(1, Ordering::Relaxed);
    EDGE_FILTERED_ROWS.fetch_add(rows, Ordering::Relaxed);
}

pub(crate) fn record_node_full_read(rows: u64) {
    NODE_FULL_READS.fetch_add(1, Ordering::Relaxed);
    NODE_FULL_ROWS.fetch_add(rows, Ordering::Relaxed);
}

pub(crate) fn record_node_filtered_read(rows: u64) {
    NODE_FILTERED_READS.fetch_add(1, Ordering::Relaxed);
    NODE_FILTERED_ROWS.fetch_add(rows, Ordering::Relaxed);
}

pub(crate) fn record_edge_scanned(rows: u64) {
    EDGE_SCANNED_ROWS.fetch_add(rows, Ordering::Relaxed);
}

pub(crate) fn record_node_scanned(rows: u64) {
    NODE_SCANNED_ROWS.fetch_add(rows, Ordering::Relaxed);
}

pub(crate) fn record_node_pruning(pruning: FilteredReadPruning) {
    match pruning.strategy {
        FilteredReadStrategy::DenseRowSelection => {
            NODE_DENSE_ROW_SELECTION_READS.fetch_add(1, Ordering::Relaxed);
        }
        FilteredReadStrategy::RowGroupPredicate => {
            NODE_ROW_GROUP_PREDICATE_READS.fetch_add(1, Ordering::Relaxed);
        }
        FilteredReadStrategy::FullFallback => {}
    }
    NODE_ROW_GROUPS_CONSIDERED.fetch_add(pruning.row_groups_considered, Ordering::Relaxed);
    NODE_ROW_GROUPS_SELECTED.fetch_add(pruning.row_groups_selected, Ordering::Relaxed);
    NODE_PAGES_CONSIDERED.fetch_add(pruning.pages_considered, Ordering::Relaxed);
    NODE_PAGES_SELECTED.fetch_add(pruning.pages_selected, Ordering::Relaxed);
    NODE_EXACT_ROWS_SELECTED.fetch_add(pruning.exact_rows_selected, Ordering::Relaxed);
    NODE_METADATA_FALLBACKS.fetch_add(pruning.metadata_fallbacks, Ordering::Relaxed);
    NODE_VALIDATION_FALLBACKS.fetch_add(pruning.validation_fallbacks, Ordering::Relaxed);
}

pub(crate) fn record_rewrite_commit() {
    REWRITE_COMMITS.fetch_add(1, Ordering::Relaxed);
}
