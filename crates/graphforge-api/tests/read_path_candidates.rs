//! #1688: the #1619 read-path candidates over the #1513 multi-file fixture.
//!
//! Requires `--features read-path-experiment`. One test owns the process
//! environment, because the candidate is read from `GF_READ_PATH_CANDIDATE`
//! whenever a session is created.

use std::collections::HashMap;

use arrow::util::pretty::pretty_format_batches;
use graphforge_api::{ExecutionResourcePolicy, GraphForge, GraphForgeOptions, ResourcePolicyMode};
use graphforge_exec::demand;
use graphforge_exec::fast_path::{READ_PATH_CANDIDATE_ENV, READ_PATH_INJECT_ENV};
use tempfile::TempDir;

#[path = "support/bulk_fixture.rs"]
mod bulk_fixture;

use bulk_fixture::{encoded_node_files, generate_bulk_graph};

const RECOUNT: &str = "MATCH ()-[r]->() RETURN count(r) AS n";
const ORDERED_ONE_HOP: &str = "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000";
const ORDERED_TWO_HOP: &str =
    "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id ORDER BY id LIMIT 1000";

/// Each query, the fast operator answering it, and its `operator_rss` label.
const FAST_PATHS: [(&str, &str, &str); 3] = [
    (RECOUNT, "EdgeCountExec", "edge_count"),
    (ORDERED_ONE_HOP, "OrderedOneHopExec", "ordered_one_hop"),
    (
        ORDERED_TWO_HOP,
        "OrderedTwoHopPathCountExec",
        "ordered_two_hop",
    ),
];

fn set_env(key: &str, value: Option<&str>) {
    // SAFETY: this binary has a single test, and no other thread reads or
    // writes the environment while it changes.
    unsafe {
        match value {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
    }
}

fn open(dir: &TempDir, partitions: usize) -> GraphForge {
    open_with(dir, partitions, ExecutionResourcePolicy::default())
}

fn open_with(dir: &TempDir, partitions: usize, base: ExecutionResourcePolicy) -> GraphForge {
    let options = GraphForgeOptions {
        resource: ExecutionResourcePolicy {
            mode: ResourcePolicyMode::Explicit,
            tokio_worker_threads: Some(1),
            target_partitions: Some(partitions),
            io_concurrency: Some(1),
            compute_threads: Some(1),
            ..base
        },
        ..GraphForgeOptions::default()
    };
    GraphForge::new_with_options(Some(dir.path().to_str().expect("UTF-8")), options)
        .unwrap_or_else(|error| panic!("target_partitions={partitions}: {error:?}"))
}

/// The plan text, the `operator_rss` labels, and the rendered result.
fn run(forge: &GraphForge, query: &str) -> (String, Vec<&'static str>, String) {
    let plan = forge.explain(query).unwrap();
    let (result, evidence) = demand::capture(|| forge.execute(query));
    let rendered = pretty_format_batches(&result.unwrap().batches)
        .unwrap()
        .to_string();
    let operators = evidence
        .operator_rss
        .into_iter()
        .map(|entry| entry.operator)
        .collect();
    (plan, operators, rendered)
}

#[test]
fn candidates_agree_and_lowerer_chosen_fast_paths_survive_shape_changes() {
    const NODES: usize = 40_000;
    const FAN_OUT: usize = 4;
    let dir = TempDir::new().unwrap();
    generate_bulk_graph(dir.path(), NODES, FAN_OUT);
    let node_files = encoded_node_files(dir.path());
    assert!(node_files > 1, "fixture spans {node_files} node files");
    let observed = std::thread::available_parallelism().map_or(1, usize::from);
    let widest = (observed.saturating_mul(2).max(4) - 1).max(node_files + 1);

    let mut answers = HashMap::new();
    for partitions in [1_usize, node_files, widest] {
        for candidate in ["current", "structural", "stock"] {
            set_env(READ_PATH_CANDIDATE_ENV, Some(candidate));
            let forge = open(&dir, partitions);
            for (query, exec, label) in FAST_PATHS {
                let context = format!("{candidate} target_partitions={partitions} {query}");
                let (plan, operators, rendered) = run(&forge, query);
                let expected = answers
                    .entry((partitions, query))
                    .or_insert_with(|| rendered.clone());
                assert_eq!(
                    &rendered, expected,
                    "{context}: answer differs from current"
                );
                if candidate == "stock" {
                    for custom in [exec, "ExpandExec", "FastPathFallbackExec"] {
                        assert!(!plan.contains(custom), "{context}: {custom} in {plan}");
                    }
                } else {
                    assert!(plan.contains(exec), "{context}: {plan}");
                    assert!(!plan.contains("FastPathFallbackExec"), "{context}: {plan}");
                    assert!(operators.contains(&label), "{context}: {operators:?}");
                }
            }
        }
    }

    // Known positive: an operator between the two expands defeats the physical
    // two-hop rewrite (it keeps the generic plan), but not the lowerer's choice.
    set_env(READ_PATH_INJECT_ENV, Some("between-expands"));
    for candidate in ["current", "structural"] {
        set_env(READ_PATH_CANDIDATE_ENV, Some(candidate));
        let forge = open(&dir, node_files);
        let (plan, _, rendered) = run(&forge, ORDERED_TWO_HOP);
        assert_eq!(
            Some(&rendered),
            answers.get(&(node_files, ORDERED_TWO_HOP)),
            "{candidate} with the injected transport"
        );
        let fast = plan.contains("OrderedTwoHopPathCountExec");
        if candidate == "current" {
            assert!(
                !fast && plan.contains("RepartitionExec") && plan.contains("ExpandExec"),
                "the injected transport must make the physical rewrite fall back: {plan}"
            );
        } else {
            assert!(fast, "the lowerer-chosen fast path must survive: {plan}");
        }
    }
    set_env(READ_PATH_INJECT_ENV, None);

    // Known positive for C's ExpandExec pool reservation: with every node in
    // one input batch and the smallest admitted pool, a generic hop that
    // returns whole rows fits when ExpandExec is unaccounted (current) and is
    // refused by the pool when it is accounted (structural).
    let whole_rows = "MATCH (a)-[r]->(b) RETURN a, r, b";
    let small_pool = ExecutionResourcePolicy {
        batch_size: Some(1_048_576),
        memory_budget_bytes: Some(16 * 1024 * 1024),
        ..ExecutionResourcePolicy::default()
    };
    set_env(READ_PATH_CANDIDATE_ENV, Some("current"));
    let rows: usize = open_with(&dir, 1, small_pool.clone())
        .execute(whole_rows)
        .expect("an unaccounted ExpandExec fits the small pool")
        .batches
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, NODES * FAN_OUT);
    set_env(READ_PATH_CANDIDATE_ENV, Some("structural"));
    let refused = open_with(&dir, 1, small_pool)
        .execute(whole_rows)
        .expect_err("an accounted ExpandExec must be refused by the small pool");
    assert!(
        format!("{refused:?}").contains("ExpandExec"),
        "the pool must name the ExpandExec reservation: {refused:?}"
    );
    set_env(READ_PATH_CANDIDATE_ENV, None);
}
