//! #1987: the caches a traversal fills are bounded by the query memory budget,
//! measured as the resident set of a separate process.
//!
//! A traversal over a graph whose adjacency index spans several CSR shards
//! decodes each shard once and keeps it for the next query. A per-index
//! constant (the former 1 GiB) retains every shard of both directions; the
//! shared read-cache budget (`graphforge_storage::read_cache_budget`) retains
//! at most `B / 4` of decoded topology, however many indexes are open.
//!
//! The graph is 1,600 nodes with 1,000 out-edges each: 1,600,000 edges. The bulk
//! build ships default-size shards (about 1M edges, larger than this whole
//! budget), so the fixture is re-indexed under the test budget: the facade then
//! sizes a shard at `budget / 192` edges (87,381 at 16 MiB), about 19 shards per
//! direction and about 1.4 MiB decoded each (16 B per edge). Few nodes keep
//! generation cheap; the decoded bytes depend on the edges.
//!
//! The query runs in a child process (this test binary re-executed on the one
//! `child` test) so the parent's graph generation does not pollute the child's
//! resident set. The child reads its own `VmHWM`, the high-water resident set,
//! from `/proc/self/status`: a deterministic in-process measurement of the
//! growth caused by the two traversals, with no sampling interval to miss a peak.

#![cfg(target_os = "linux")]

use std::path::Path;
use std::process::Command;

use arrow::array::Int64Array;
use graphforge_api::{ExecutionResourcePolicy, GraphForge, GraphForgeOptions, ResourcePolicyMode};

#[path = "support/bulk_fixture.rs"]
mod bulk_fixture;

use bulk_fixture::generate_bulk_graph_with_index;

const NODES: usize = 1_600;
const FAN_OUT: usize = 1_000;
const EDGES: i64 = (NODES * FAN_OUT) as i64;
/// The facade's smallest explicit query memory budget.
const BUDGET_BYTES: u64 = 16 * 1024 * 1024;
/// Resident growth during the two traversals must stay within this multiple of
/// the query memory budget `B`. It is derived, not fitted to a measurement:
///
/// * the query pool, charged by every operator that registers with it: `B`;
/// * the retained read caches (decoded shards, label index, identity blocks):
///   `B / 2` by the shared formula, however many shards the index has;
/// * one shard being decoded, which the cache admits even when it alone exceeds
///   its share: `budget / 192` edges at 16 B is `B / 12`, under `B / 8`.
///
/// That is under `1.7 B`, rounded up to 2x. Retaining every shard of both
/// directions (the per-index constant this replaced) holds 1.6M edges x 16 B x 2
/// directions = 51 MB decoded, about `3 B`, on top of the traversal's own
/// footprint, and fails the assertion.
const RSS_GROWTH_MULTIPLE: u64 = 2;
const CHILD_ENV: &str = "GF_READ_CACHE_CHILD";

const OUT_QUERY: &str = "MATCH (a:Entity)-[:LINK]->(b) RETURN count(*) AS n";
const IN_QUERY: &str = "MATCH (b)<-[:LINK]-(a) RETURN count(*) AS n";

fn status_kib(key: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix(key))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| panic!("no {key} in /proc/self/status"))
}

fn open(dir: &Path, memory_budget_bytes: u64) -> GraphForge {
    GraphForge::new_with_options(
        Some(dir.to_str().expect("temp path is UTF-8")),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                mode: ResourcePolicyMode::Explicit,
                tokio_worker_threads: Some(2),
                target_partitions: Some(2),
                io_concurrency: Some(2),
                compute_threads: Some(1),
                memory_budget_bytes: Some(memory_budget_bytes),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .unwrap()
}

fn count(forge: &GraphForge, query: &str) -> i64 {
    let result = forge.execute(query).unwrap();
    let values: Vec<i64> = result
        .batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("count is Int64")
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(values.len(), 1, "{query}: {values:?}");
    values[0]
}

/// The child: run both traversals over `$GF_READ_CACHE_CHILD` (`path|budget`)
/// and print one `RSS {json}` line. It does nothing outside a child process.
#[test]
fn child() {
    let Ok(spec) = std::env::var(CHILD_ENV) else {
        return;
    };
    let (path, budget) = spec.split_once('|').unwrap();
    let forge = open(Path::new(path), budget.parse().unwrap());
    for query in [OUT_QUERY, IN_QUERY] {
        let plan = forge.explain(query).unwrap();
        assert!(
            plan.lines()
                .any(|line| line.contains("ExpandExec") && line.contains("adjacency=hit")),
            "{query} must read the CSR index, not a count fast path:\n{plan}"
        );
    }
    let start_rss = status_kib("VmRSS:") * 1024;
    let out = count(&forge, OUT_QUERY);
    let into = count(&forge, IN_QUERY);
    let line = serde_json::json!({
        "out": out,
        "into": into,
        "start_rss_bytes": start_rss,
        "peak_rss_bytes": status_kib("VmHWM:") * 1024,
    });
    println!("RSS {line}");
}

#[test]
fn traversing_a_multi_shard_index_stays_within_a_multiple_of_the_memory_budget() {
    let project = tempfile::TempDir::new().unwrap();
    generate_bulk_graph_with_index(project.path(), NODES, FAN_OUT, false);
    open(project.path(), BUDGET_BYTES)
        .index_adjacency()
        .unwrap();

    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child", "--nocapture", "--test-threads", "1"])
        .env(
            CHILD_ENV,
            format!("{}|{BUDGET_BYTES}", project.path().display()),
        )
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "child failed:\n{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    // libtest prints `test child ... ` before the first line a test writes.
    let line = stdout
        .lines()
        .find_map(|line| line.split_once("RSS ").map(|(_, report)| report))
        .unwrap_or_else(|| panic!("no RSS line in child output:\n{stdout}"));
    let report: serde_json::Value = serde_json::from_str(line).unwrap();

    assert_eq!(report["out"], EDGES, "out-expansion row count");
    assert_eq!(report["into"], EDGES, "in-expansion row count");
    let start = report["start_rss_bytes"].as_u64().unwrap();
    let peak = report["peak_rss_bytes"].as_u64().unwrap();
    let growth = peak.saturating_sub(start);
    let allowed = RSS_GROWTH_MULTIPLE * BUDGET_BYTES;
    println!("resident growth {growth} bytes, allowed {allowed}: {line}");
    assert!(
        growth <= allowed,
        "{line}: resident growth {growth} bytes exceeds {RSS_GROWTH_MULTIPLE}x the {BUDGET_BYTES} byte \
         query memory budget ({allowed} bytes): the read caches are not bounded by it"
    );
}
