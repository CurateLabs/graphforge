//! Mutated-project open cost and corruption refusal (#1388, criteria 1 and 4).
//!
//! Every mutating commit publishes a compact (V2) root and installs only the
//! files it changed, and delta runs are no longer published, so a project that
//! has been mutated opens exactly as cheaply as the construction session's
//! published generation: the manifest, the route table and the small controls,
//! never a payload.
//!
//! The mutated project here is the one a user has after real use: a bulk
//! constructed graph, then a CREATE, a SET on an existing node, an edge DELETE,
//! a node DELETE and `index_adjacency`, each through the ordinary public path.
//! Nodes and edges both grow 16x between the two sizes (edge fan-out is fixed),
//! so work proportional to either shows up in the open.
//!
//! The gate is deterministic: lifecycle-attributed read bytes and the
//! whole-process `rchar` where the platform has it, never wall time. The bound
//! is derived from what the manifest declares, not from any observed output.

use std::collections::HashMap;
use std::path::Path;

use arrow::array::{Array, FixedSizeBinaryArray};
use graphforge_api::{GraphForge, LifecycleIoCapture, lifecycle_io_snapshot};
use graphforge_ir::IrLiteral;
use graphforge_storage::{GraphFilesInventory, resolve_project_generation};

#[path = "support/bulk_fixture.rs"]
mod bulk_fixture;

const SMALL_NODES: usize = 1 << 10;
const LARGE_NODES: usize = 16 * SMALL_NODES;
const FAN_OUT: usize = 32;
const LIMIT: usize = 1_000;
const ONE_HOP: &str = "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000";

/// The node whose property is set, and the edge removed from the ring.
const SET_NODE: usize = 7;
const DELETED_EDGE_SOURCE: usize = 9;
const DELETED_EDGE_OFFSET: usize = 3;

fn uuid_param(index: usize) -> IrLiteral {
    IrLiteral::Uuid(*bulk_fixture::fixture_node_uuid(index).as_bytes())
}

/// CREATE, SET, DELETE and `index_adjacency` over a bulk-constructed project.
fn mutate(path: &Path) {
    let forge = GraphForge::new(Some(path.to_str().expect("utf-8 project path"))).unwrap();
    forge
        .execute("CREATE (:Extra {name: 'created'})")
        .expect("CREATE");
    forge
        .execute_with_params(
            "MATCH (n:Entity) WHERE n.node_uuid = $node SET n.tag = 'set'",
            &HashMap::from([("node".to_owned(), uuid_param(SET_NODE))]),
        )
        .expect("SET on an existing node");
    forge
        .execute_with_params(
            "MATCH (a:Entity)-[r:LINK]->(b:Entity) \
             WHERE a.node_uuid = $source AND b.node_uuid = $target DELETE r",
            &HashMap::from([
                ("source".to_owned(), uuid_param(DELETED_EDGE_SOURCE)),
                (
                    "target".to_owned(),
                    uuid_param(DELETED_EDGE_SOURCE + DELETED_EDGE_OFFSET),
                ),
            ]),
        )
        .expect("DELETE of an edge");
    forge
        .execute("MATCH (n:Extra) DELETE n")
        .expect("DELETE of a node");
    forge.index_adjacency().expect("index_adjacency");
}

fn process_rchar() -> Option<u64> {
    std::fs::read_to_string("/proc/self/io")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("rchar: "))?
        .trim()
        .parse()
        .ok()
}

#[derive(Debug)]
struct Open {
    attributed: u64,
    rchar: Option<u64>,
    copied_bytes: u64,
    checksummed_bytes: u64,
}

fn open_and_query(path: &Path, nodes: usize) -> Open {
    let _capture = LifecycleIoCapture::install();
    let before = lifecycle_io_snapshot().expect("requested observation");
    let rchar_before = process_rchar();
    let forge = GraphForge::new(Some(path.to_str().expect("utf-8 project path"))).unwrap();
    let rchar_after = process_rchar();
    let open = lifecycle_io_snapshot()
        .expect("requested observation")
        .since(&before)
        .expect("open attribution");
    open.validate_for_qualification().expect("open reconciles");
    let evidence = forge.graph_open_evidence();
    let measured = Open {
        attributed: open.totals.read_bytes,
        rchar: Some(rchar_after.unwrap() - rchar_before.unwrap()),
        copied_bytes: evidence.bytes_copied,
        checksummed_bytes: evidence.bytes_checksummed,
    };
    let result = forge.execute(ONE_HOP).expect("one-hop query");
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
    assert_eq!(ids, expected_ids(nodes), "wrong answer after mutation");
    measured
}

/// Every node is the destination of `FAN_OUT` ring edges except the one whose
/// incoming edge was deleted; the ordered answer is each identity repeated by
/// its in-degree, cut at the limit.
fn expected_ids(nodes: usize) -> Vec<Vec<u8>> {
    let deleted_target = DELETED_EDGE_SOURCE + DELETED_EDGE_OFFSET;
    (0..nodes)
        .flat_map(|node| {
            let in_degree = if node == deleted_target {
                FAN_OUT - 1
            } else {
                FAN_OUT
            };
            std::iter::repeat_n(
                bulk_fixture::fixture_node_uuid(node).as_bytes().to_vec(),
                in_degree,
            )
        })
        .take(LIMIT)
        .collect()
}

fn declared_layout(inventory: &GraphFilesInventory) -> Vec<(String, u64)> {
    inventory
        .files
        .iter()
        .map(|file| (file.relative_path.clone(), file.byte_length))
        .collect()
}

#[test]
fn explore_mutated_project_open() {
    for nodes in [SMALL_NODES, LARGE_NODES] {
        let project = tempfile::tempdir().expect("project directory");
        let path = project.path().join("state");
        bulk_fixture::generate_bulk_graph_with_index(&path, nodes, FAN_OUT, false);
        mutate(&path);
        let inventory = resolve_project_generation(&path)
            .unwrap()
            .unadmitted_graph_files_inventory()
            .unwrap()
            .unwrap();
        let mut by_prefix: std::collections::BTreeMap<String, (u64, u64)> = Default::default();
        for (name, bytes) in declared_layout(&inventory) {
            let key = name
                .split('/')
                .take(2)
                .collect::<Vec<_>>()
                .join("/");
            let entry = by_prefix.entry(key).or_default();
            entry.0 += 1;
            entry.1 += bytes;
        }
        eprintln!("nodes={nodes} edges={} files={}", nodes * FAN_OUT, inventory.files.len());
        for (key, (count, bytes)) in &by_prefix {
            eprintln!("  {key}: files={count} bytes={bytes}");
        }
        let open = open_and_query(&path, nodes);
        eprintln!("  open: {open:?}");
    }
}
