//! #1586 measurement: query latency while imports run, per construction
//! reserve. Ignored; run alone, release build, pinned to the CPUs the policy
//! claims, on a quiet host:
//!
//! ```text
//! TMPDIR=<ext4 dir> GF_1586_COMPUTE=4 taskset -c 0-3 cargo test --release -p graphforge-api --lib \
//!   import_session::cpu_budget_report -- --ignored --nocapture --test-threads=1
//! ```
//!
//! One project holds a committed base graph. For each configuration a fresh
//! instance opens it with `GF_1586_COMPUTE` compute threads, runs a fixed `rank` (PageRank)
//! workload alone, then runs the same workload while two imports validate
//! concurrently on the same instance (they are aborted, so the project never
//! changes). Configurations rotate every round. `unbounded` replaces the
//! instance admission with a 64-lane one, which is how construction behaved
//! before #1586. Each observation is printed as one `CPU_BUDGET_REPORT` JSON
//! line.
//!
//! Knobs: `GF_1586_COMPUTE` (compute threads, default 4), `GF_1586_EDGES` (edges per import, default 2,000,000),
//! `GF_1586_BASE_EDGES` (default 400,000), `GF_1586_ROUNDS` (default 3),
//! `GF_1586_ALONE_QUERIES` (default 10).

use super::*;
use crate::{GraphForgeOptions, bulk_edge_input_schema, bulk_node_input_schema};
use arrow::array::{FixedSizeBinaryArray, StringArray};
use graphforge_core::RankOptions;
use graphforge_core::algorithms::RankAlgorithm;
use std::sync::Arc;
use std::time::{Duration, Instant};

const BATCH_ROWS: usize = 65_536;

fn knob(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn uuid_column(prefix: u128, indexes: impl Iterator<Item = u64>) -> Arc<FixedSizeBinaryArray> {
    let values = indexes
        .map(|index| (Uuid::from_u128(prefix | u128::from(index))).into_bytes())
        .collect::<Vec<_>>();
    Arc::new(FixedSizeBinaryArray::try_from_iter(values.iter()).unwrap())
}

const NODE_PREFIX: u128 = 0x0190_0000_0000_7000_8000_0000_0000_0000;

/// Deterministic node and edge batches. `edge_prefix` separates the edge
/// identity spaces of different inputs.
fn input(nodes: u64, edges: u64, edge_prefix: u128) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
    let node_batches = (0..nodes)
        .step_by(BATCH_ROWS)
        .map(|start| {
            let end = (start + BATCH_ROWS as u64).min(nodes);
            RecordBatch::try_new(
                bulk_node_input_schema(Vec::new()).unwrap(),
                vec![
                    uuid_column(NODE_PREFIX, start..end),
                    Arc::new(StringArray::from(vec!["Node"; (end - start) as usize])),
                ],
            )
            .unwrap()
        })
        .collect();
    // A fixed LCG keeps every configuration on identical inputs.
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut next = move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (state >> 33) % nodes
    };
    let edge_batches = (0..edges)
        .step_by(BATCH_ROWS)
        .map(|start| {
            let end = (start + BATCH_ROWS as u64).min(edges);
            let count = (end - start) as usize;
            let sources = (0..count).map(|_| next()).collect::<Vec<_>>();
            let targets = (0..count).map(|_| next()).collect::<Vec<_>>();
            RecordBatch::try_new(
                bulk_edge_input_schema(Vec::new()).unwrap(),
                vec![
                    uuid_column(edge_prefix, start..end),
                    Arc::new(StringArray::from(vec!["LINKS"; count])),
                    uuid_column(NODE_PREFIX, sources.into_iter()),
                    uuid_column(NODE_PREFIX, targets.into_iter()),
                ],
            )
            .unwrap()
        })
        .collect();
    (node_batches, edge_batches)
}

fn options(compute: usize, reserve: Option<usize>) -> GraphForgeOptions {
    GraphForgeOptions {
        resource: crate::ExecutionResourcePolicy {
            mode: crate::ResourcePolicyMode::Explicit,
            tokio_worker_threads: Some(compute),
            compute_threads: Some(compute),
            construction_cpu_reserve: reserve,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn query(graph: &GraphForge) -> Duration {
    let started = Instant::now();
    let ranked = graph
        .rank(
            "Node",
            RankOptions {
                by: RankAlgorithm::PageRank,
                via: None,
                directed: true,
                write_property: None,
            },
        )
        .unwrap();
    assert!(ranked.num_rows() > 0);
    started.elapsed()
}

fn millis(durations: &[Duration]) -> serde_json::Value {
    let mut sorted = durations
        .iter()
        .map(|duration| duration.as_secs_f64() * 1e3)
        .collect::<Vec<_>>();
    sorted.sort_by(f64::total_cmp);
    let at = |q: f64| sorted[((sorted.len() - 1) as f64 * q).round() as usize];
    serde_json::json!({
        "n": sorted.len(),
        "p50_ms": at(0.5),
        "p95_ms": at(0.95),
        "max_ms": sorted[sorted.len() - 1],
    })
}

fn import_and_abort(graph: &GraphForge, nodes: &[RecordBatch], edges: &[RecordBatch]) -> Duration {
    let started = Instant::now();
    let mut session = graph
        .begin_import_session(
            OperationId(Uuid::now_v7()),
            ImportSessionLimits {
                batch_rows: BATCH_ROWS,
                ..ImportSessionLimits::default()
            },
        )
        .unwrap();
    session.append_arrow(BulkInputKind::Node, nodes).unwrap();
    session.append_arrow(BulkInputKind::Edge, edges).unwrap();
    session.validate(graph).unwrap();
    let elapsed = started.elapsed();
    session.abort(graph).unwrap();
    elapsed
}

#[test]
#[ignore = "measurement; run alone on a quiet host, see module docs"]
fn construction_cpu_reserve_report() {
    let edges = knob("GF_1586_EDGES", 2_000_000) as u64;
    let base_edges = knob("GF_1586_BASE_EDGES", 400_000) as u64;
    let rounds = knob("GF_1586_ROUNDS", 3);
    let alone_queries = knob("GF_1586_ALONE_QUERIES", 10);
    let compute = knob("GF_1586_COMPUTE", 4);
    let directory = tempfile::tempdir().unwrap();
    let project = directory.path().join("project");
    fs::create_dir(&project).unwrap();
    {
        let graph = GraphForge::new_with_options(project.to_str(), options(compute, None)).unwrap();
        let (nodes, edges) = input(base_edges / 8, base_edges, 0x0191 << 112);
        let mut session = graph
            .begin_import_session(
                OperationId(Uuid::now_v7()),
                ImportSessionLimits {
                    batch_rows: BATCH_ROWS,
                    ..ImportSessionLimits::default()
                },
            )
            .unwrap();
        session.append_arrow(BulkInputKind::Node, &nodes).unwrap();
        session.append_arrow(BulkInputKind::Edge, &edges).unwrap();
        session.validate(&graph).unwrap();
        session.commit(&graph, None).unwrap();
    }
    let (first_nodes, first_edges) = input(edges / 8, edges, 0x0192 << 112);
    let (second_nodes, second_edges) = input(edges / 8, edges, 0x0193 << 112);
    // Unbounded, then reserve 1, a quarter and a half of the compute threads.
    let mut configurations = vec![("unbounded".to_owned(), None)];
    let mut reserves = vec![1, compute / 4, compute / 2];
    reserves.retain(|reserve| *reserve >= 1 && *reserve < compute);
    reserves.dedup();
    configurations.extend(
        reserves
            .into_iter()
            .map(|reserve| (format!("reserve-{reserve}"), Some(reserve))),
    );
    for round in 0..rounds {
        for offset in 0..configurations.len() {
            let (name, reserve) = configurations[(round + offset) % configurations.len()].clone();
            let mut graph = GraphForge::new_with_options(
                project.to_str(),
                options(compute, reserve.or(Some(1))),
            )
            .unwrap();
            if reserve.is_none() {
                graph.construction_cpu_admission =
                    Arc::new(graphforge_storage::ConstructionCpuAdmission::new(
                        std::num::NonZeroUsize::new(64).unwrap(),
                    ));
            }
            let graph = &graph;
            query(graph); // warm the adjacency index outside the measurement
            let alone = (0..alone_queries).map(|_| query(graph)).collect::<Vec<_>>();
            let started = Instant::now();
            let (during, imports) = std::thread::scope(|scope| {
                let first = scope.spawn(|| import_and_abort(graph, &first_nodes, &first_edges));
                let second = scope.spawn(|| import_and_abort(graph, &second_nodes, &second_edges));
                let mut during = Vec::new();
                while !(first.is_finished() && second.is_finished()) {
                    during.push(query(graph));
                }
                (during, [first.join().unwrap(), second.join().unwrap()])
            });
            let diagnostics = graph.resource_diagnostics();
            println!(
                "CPU_BUDGET_REPORT {}",
                serde_json::json!({
                    "round": round + 1,
                    "configuration": name,
                    "construction_cpu_limit": diagnostics.construction_cpu_limit,
                    "construction_cpu_peak": diagnostics.construction_cpu_peak,
                    "compute_threads": diagnostics.compute_threads,
                    "edges_per_import": edges,
                    "base_edges": base_edges,
                    "alone": millis(&alone),
                    "during_imports": millis(&during),
                    "import_wall_s": imports.map(|wall| wall.as_secs_f64()),
                    "window_wall_s": started.elapsed().as_secs_f64(),
                })
            );
        }
    }
}
