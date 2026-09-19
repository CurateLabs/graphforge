//! Divan timing workloads for node MERGE scaling (#1485).
//!
//! Deterministic topology-read assertions remain in `tests/merge_scaling_bench.rs`.
//! Run with: `cargo bench -p graphforge-exec --bench merge_scaling -- --sample-count 5`

mod scaling_common;

use divan::Bencher;
use scaling_common::merge::FreshMergeFixture;

fn main() {
    divan::main();
}

const MERGE_ROWS: usize = 200;

/// Timed region includes fresh filler-graph construction plus the MERGE so each
/// sample starts from an empty `Merged` label state.
#[divan::bench(args = [2_000, 40_000], sample_count = 5, sample_size = 1)]
fn node_merge_over_filler_graph(bencher: Bencher, filler_nodes: usize) {
    bencher.bench(|| {
        FreshMergeFixture::with_filler_nodes(filler_nodes).merge_rows(MERGE_ROWS);
    });
}
