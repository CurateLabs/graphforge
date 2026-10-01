//! Node-term measurement for #1388 range authentication (prototype).
//!
//! Same canonical ordered one-hop and two-hop `LIMIT 1000` queries as
//! `bounded_query_cost.rs`, at two node counts 8x apart with fan-out fixed,
//! so work proportional to the node payload shows up as the node count grows.
//! Prints declared node bytes, the largest CSR shard, open and execution read
//! bytes and wall time; asserts the answer and the loose structural bound
//! (nodes + shards + residual) so the same file runs on the baseline tree.
//! The strict no-node-term assertion lives in `range_authentication_mutation.rs`.

use std::collections::BTreeMap;
use std::time::Instant;

use arrow::array::{Array, FixedSizeBinaryArray};
use graphforge_api::{GraphForge, LifecycleIoCapture, lifecycle_io_snapshot};
use graphforge_storage::{GraphFilesInventory, resolve_project_generation};

#[path = "support/bulk_fixture.rs"]
mod bulk_fixture;

const FAN_OUT: usize = 8;
const LIMIT: usize = 1_000;
const ONE_HOP: &str = "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000";
const TWO_HOP: &str =
    "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id ORDER BY id LIMIT 1000";
const EXECUTION_RESIDUAL_BYTES: u64 = 16 * 1024;

struct Layout {
    node_bytes: u64,
    node_objects: u64,
    edge_bytes: u64,
    largest_shard_bytes: u64,
}

fn layout(inventory: &GraphFilesInventory) -> Layout {
    let mut by_class = BTreeMap::<&str, u64>::new();
    let mut largest_shard_bytes = 0;
    let mut node_objects = 0;
    for file in &inventory.files {
        let path = file.relative_path.as_str();
        if path.contains(".csr.shards-") && path.ends_with(".csr") {
            largest_shard_bytes = largest_shard_bytes.max(file.byte_length);
        } else if path.starts_with("topology/nodes/") {
            node_objects += 1;
            *by_class.entry("nodes").or_default() += file.byte_length;
        } else if path.starts_with("topology/edges/") {
            *by_class.entry("edges").or_default() += file.byte_length;
        }
    }
    Layout {
        node_bytes: by_class["nodes"],
        node_objects,
        edge_bytes: by_class["edges"],
        largest_shard_bytes,
    }
}

fn expected_ids(nodes: usize, paths_per_destination: usize) -> Vec<Vec<u8>> {
    (0..nodes)
        .flat_map(|node| {
            std::iter::repeat_n(
                bulk_fixture::fixture_node_uuid(node).as_bytes().to_vec(),
                paths_per_destination,
            )
        })
        .take(LIMIT)
        .collect()
}

fn run(nodes: usize) {
    let project = tempfile::tempdir().expect("project directory");
    let path = project.path().join("state");
    let built = Instant::now();
    bulk_fixture::generate_bulk_graph_with_index(&path, nodes, FAN_OUT, false);
    let build_wall = built.elapsed();
    let inventory = resolve_project_generation(&path)
        .expect("project resolves")
        .unadmitted_graph_files_inventory()
        .expect("inventory reads")
        .expect("compact generation declares an inventory");
    let layout = layout(&inventory);
    eprintln!(
        "nodes={nodes} edges={} node_objects={} node_bytes={} edge_bytes={} largest_shard_bytes={} build_s={:.1}",
        nodes * FAN_OUT,
        layout.node_objects,
        layout.node_bytes,
        layout.edge_bytes,
        layout.largest_shard_bytes,
        build_wall.as_secs_f64()
    );
    for (name, text, hops, paths) in [
        ("one-hop", ONE_HOP, 1_u64, FAN_OUT),
        ("two-hop", TWO_HOP, 2_u64, FAN_OUT * FAN_OUT),
    ] {
        let _capture = LifecycleIoCapture::install();
        let before_open = lifecycle_io_snapshot().expect("requested observation");
        let started = Instant::now();
        let forge = GraphForge::new(Some(path.to_str().expect("utf-8 project path"))).unwrap();
        let open_wall = started.elapsed();
        let after_open = lifecycle_io_snapshot().expect("requested observation");
        let open = after_open.since(&before_open).expect("open attribution");

        let started = Instant::now();
        let result = forge.execute(text).unwrap();
        let execution_wall = started.elapsed();
        let execution = lifecycle_io_snapshot()
            .expect("requested observation")
            .since(&after_open)
            .expect("execution attribution");

        let mut ids = Vec::new();
        for batch in &result.batches {
            let column = batch
                .column_by_name("id")
                .expect("id column")
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("node_uuid is FixedSizeBinary");
            ids.extend((0..batch.num_rows()).map(|row| column.value(row).to_vec()));
        }
        assert_eq!(
            ids,
            expected_ids(nodes, paths),
            "{name} nodes={nodes}: wrong answer"
        );

        let exec_read = execution.totals.read_bytes;
        let strict_bound = hops * layout.largest_shard_bytes + EXECUTION_RESIDUAL_BYTES;
        let loose_bound = layout.node_bytes + strict_bound;
        eprintln!(
            "  {name}: open_read={} exec_read={exec_read} strict_bound(no node term)={strict_bound} \
             loose_bound={loose_bound} node_term_paid={} open_ms={:.1} exec_ms={:.1}",
            open.totals.read_bytes,
            exec_read.saturating_sub(strict_bound),
            open_wall.as_secs_f64() * 1e3,
            execution_wall.as_secs_f64() * 1e3,
        );
        assert!(
            exec_read <= loose_bound,
            "{name} nodes={nodes}: execution read {exec_read} exceeds even the loose bound {loose_bound}"
        );
    }
}

#[test]
fn bounded_queries_at_16k_nodes() {
    run(1 << 14);
}

#[test]
fn bounded_queries_at_128k_nodes() {
    run(1 << 17);
}
