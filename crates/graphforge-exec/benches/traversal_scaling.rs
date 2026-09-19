//! Divan timing workloads for localized and scattered traversal scaling (#1485).
//!
//! Deterministic I/O scaling assertions remain in
//! `tests/bench_traversal_scaling.rs`. Run this target with:
//! `cargo bench -p graphforge-exec --bench traversal_scaling -- --sample-count 5`

mod scaling_common;

use divan::Bencher;
use scaling_common::traversal::{WarmTraversalFixture, env_usize};

fn main() {
    divan::main();
}

const FAN_OUT: usize = 16;

#[divan::bench(sample_count = 5, sample_size = 1)]
fn localized_small_one_hop(bencher: Bencher) {
    let nodes = env_usize("GF_BENCH_N1", 62_500);
    bencher
        .with_inputs(|| WarmTraversalFixture::localized(nodes, FAN_OUT, 8))
        .bench_local_refs(|fixture| fixture.expand_hops(1));
}

#[divan::bench(sample_count = 5, sample_size = 1)]
fn localized_small_two_hop(bencher: Bencher) {
    let nodes = env_usize("GF_BENCH_N1", 62_500);
    bencher
        .with_inputs(|| WarmTraversalFixture::localized(nodes, FAN_OUT, 8))
        .bench_local_refs(|fixture| fixture.expand_hops(2));
}

#[divan::bench(sample_count = 5, sample_size = 1)]
fn localized_small_three_hop(bencher: Bencher) {
    let nodes = env_usize("GF_BENCH_N1", 62_500);
    bencher
        .with_inputs(|| WarmTraversalFixture::localized(nodes, FAN_OUT, 8))
        .bench_local_refs(|fixture| fixture.expand_hops(3));
}

#[divan::bench(sample_count = 5, sample_size = 1)]
fn localized_large_one_hop(bencher: Bencher) {
    let nodes = env_usize("GF_BENCH_N2", 625_000);
    bencher
        .with_inputs(|| WarmTraversalFixture::localized(nodes, FAN_OUT, 8))
        .bench_local_refs(|fixture| fixture.expand_hops(1));
}

#[divan::bench(sample_count = 5, sample_size = 1)]
fn localized_large_two_hop(bencher: Bencher) {
    let nodes = env_usize("GF_BENCH_N2", 625_000);
    bencher
        .with_inputs(|| WarmTraversalFixture::localized(nodes, FAN_OUT, 8))
        .bench_local_refs(|fixture| fixture.expand_hops(2));
}

#[divan::bench(sample_count = 5, sample_size = 1)]
fn localized_large_three_hop(bencher: Bencher) {
    let nodes = env_usize("GF_BENCH_N2", 625_000);
    bencher
        .with_inputs(|| WarmTraversalFixture::localized(nodes, FAN_OUT, 8))
        .bench_local_refs(|fixture| fixture.expand_hops(3));
}

#[divan::bench(sample_count = 5, sample_size = 1)]
fn scattered_small_one_hop(bencher: Bencher) {
    let nodes = env_usize("GF_BENCH_N1", 62_500);
    bencher
        .with_inputs(|| WarmTraversalFixture::scattered(nodes, FAN_OUT, 64))
        .bench_local_refs(|fixture| fixture.expand_hops(1));
}

#[divan::bench(sample_count = 5, sample_size = 1)]
fn scattered_small_three_hop(bencher: Bencher) {
    let nodes = env_usize("GF_BENCH_N1", 62_500);
    bencher
        .with_inputs(|| WarmTraversalFixture::scattered(nodes, FAN_OUT, 64))
        .bench_local_refs(|fixture| fixture.expand_hops(3));
}

#[divan::bench(sample_count = 5, sample_size = 1)]
fn scattered_large_one_hop(bencher: Bencher) {
    let nodes = env_usize("GF_BENCH_N2", 625_000);
    bencher
        .with_inputs(|| WarmTraversalFixture::scattered(nodes, FAN_OUT, 64))
        .bench_local_refs(|fixture| fixture.expand_hops(1));
}

#[divan::bench(sample_count = 5, sample_size = 1)]
fn scattered_large_three_hop(bencher: Bencher) {
    let nodes = env_usize("GF_BENCH_N2", 625_000);
    bencher
        .with_inputs(|| WarmTraversalFixture::scattered(nodes, FAN_OUT, 64))
        .bench_local_refs(|fixture| fixture.expand_hops(3));
}
