//! Operations that must read everything still do (#1388, criterion 5).
//!
//! Bounded queries stopped paying for the whole project by not reading what
//! they do not return. The converse must hold too: an operation whose answer
//! is the whole payload reads the whole payload, at every size. Nothing here
//! may get cheaper by skipping data it returns.
//!
//! Nodes and edges scale 16x together, and every node and edge carries a
//! property. At each size:
//!
//! - a full property scan (`RETURN n.name`, no `LIMIT`) returns every value and
//!   reads at least the bytes the manifest declares for the node property
//!   fragments, and the edge property scan the same for edge properties;
//! - a complete portable export reads at least the node, edge and property
//!   payload the manifest declares, writes at least that payload, and reads at
//!   least the payload bytes it writes.
//!
//! Every read bound comes from the manifest, not from any observation or any
//! re-encoded size; the declared payload growing with the data is what makes the
//! reads scale. Reads are the whole-process `rchar`, which counts every
//! `read(2)` whoever issues it, so a reader that forgot to report to the
//! lifecycle attribution cannot make an operation look cheaper. Recount is not
//! asserted here.
//!
//! This file holds exactly one test on purpose: `rchar` counts the whole
//! process, so a second test on another thread would be charged to this one.

#![cfg(all(target_os = "linux", feature = "portable"))]

use std::collections::BTreeSet;
use std::path::Path;

use arrow::array::{Array, StringArray};
use graphforge_api::{
    GraphForge, PortableSelection, PortableV2ExportRequest, PortableV2Limits, PortableV2Output,
    PortableV2SelectionProfile,
};
use graphforge_storage::resolve_project_generation;

#[path = "support/bulk_fixture.rs"]
mod bulk_fixture;

const SMALL_NODES: usize = 1_024;
const FAN_OUT: usize = 4;
const SCALE: usize = 16;

/// Bytes the whole process has asked the kernel to read.
fn rchar() -> u64 {
    std::fs::read_to_string("/proc/self/io")
        .expect("/proc/self/io")
        .lines()
        .find_map(|line| line.strip_prefix("rchar: "))
        .expect("rchar line")
        .trim()
        .parse()
        .expect("rchar value")
}

/// Declared bytes by payload class, read from the manifest without admitting
/// a byte.
#[derive(Debug, Default)]
struct Declared {
    nodes: u64,
    edges: u64,
    node_properties: u64,
    edge_properties: u64,
}

fn declared(project: &Path) -> Declared {
    let inventory = resolve_project_generation(project)
        .expect("project resolves")
        .unadmitted_graph_files_inventory()
        .expect("inventory reads")
        .expect("compact generation declares an inventory");
    let mut declared = Declared::default();
    for file in &inventory.files {
        let path = file.relative_path.as_str();
        let class = if path.starts_with("topology/nodes/") {
            &mut declared.nodes
        } else if path.starts_with("topology/edges/") {
            &mut declared.edges
        } else if path.starts_with("properties/") {
            &mut declared.node_properties
        } else if path.starts_with("edge_properties/") {
            &mut declared.edge_properties
        } else {
            continue;
        };
        *class += file.byte_length;
    }
    declared
}

/// Every value of the one string column `column`, and the process bytes read
/// to produce them.
fn scan(forge: &GraphForge, query: &str, column: &str) -> (BTreeSet<String>, usize, u64) {
    let started = rchar();
    let result = forge.execute(query).expect(query);
    let read = rchar() - started;
    let mut values = BTreeSet::new();
    let mut rows = 0;
    for batch in &result.batches {
        let array = batch
            .column_by_name(column)
            .expect("column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 column");
        rows += array.len();
        values.extend((0..array.len()).map(|row| array.value(row).to_owned()));
    }
    (values, rows, read)
}

struct Size {
    nodes: usize,
    declared: Declared,
    node_scan_read: u64,
    edge_scan_read: u64,
    export_read: u64,
    export_payload: u64,
}

fn measure(root: &Path, nodes: usize) -> Size {
    let path = root.join(format!("state-{nodes}"));
    bulk_fixture::generate_bulk_graph_with_properties(&path, nodes, FAN_OUT, true);
    let declared = declared(&path);
    let edges = nodes * FAN_OUT;
    // Each scan opens its own project, so neither inherits the other's admitted
    // fragments.
    let (node_names, rows, node_scan_read) = scan(
        &GraphForge::new(path.to_str()).unwrap(),
        "MATCH (n) RETURN n.name AS name",
        "name",
    );
    assert_eq!(rows, nodes, "every node returns its name");
    assert_eq!(
        node_names,
        (0..nodes).map(bulk_fixture::fixture_node_name).collect(),
        "the names are the input's"
    );
    let (edge_notes, rows, edge_scan_read) = scan(
        &GraphForge::new(path.to_str()).unwrap(),
        "MATCH ()-[r]->() RETURN r.note AS note",
        "note",
    );
    assert_eq!(rows, edges, "every edge returns its note");
    assert_eq!(
        edge_notes,
        (0..edges).map(bulk_fixture::fixture_edge_note).collect(),
        "the notes are the input's"
    );

    let forge = GraphForge::new(path.to_str()).unwrap();
    let package = root.join(format!("export-{nodes}.gfpb"));
    let started = rchar();
    let receipt = forge
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: package,
                representation: PortableV2Output::Bundle,
                profile: PortableV2SelectionProfile::Complete,
                subset: None,
                limits: PortableV2Limits::default(),
            },
            None,
            |_| {},
        )
        .expect("complete export");
    let export_read = rchar() - started;

    // Not asserted: recount is answered from metadata (`EdgeCountExec`).
    let started = rchar();
    forge
        .execute("MATCH ()-[r]->() RETURN count(r) AS total")
        .expect("recount");
    let recount_read = rchar() - started;

    let size = Size {
        nodes,
        declared,
        node_scan_read,
        edge_scan_read,
        export_read,
        export_payload: receipt.payload_bytes,
    };
    eprintln!(
        "nodes={nodes} edges={edges} declared={:?} node_scan_read={} edge_scan_read={} \
         export_read={} export_payload={} recount_read={recount_read} (not asserted)",
        size.declared,
        size.node_scan_read,
        size.edge_scan_read,
        size.export_read,
        size.export_payload,
    );
    size
}

#[test]
fn whole_payload_operations_read_the_whole_payload_at_every_size() {
    let root = tempfile::tempdir().expect("project directory");
    let small = measure(root.path(), SMALL_NODES);
    let large = measure(root.path(), SMALL_NODES * SCALE);

    // The data grew with both axes; the lower bounds below grow with it.
    for (what, small, large) in [
        ("node", small.declared.nodes, large.declared.nodes),
        ("edge", small.declared.edges, large.declared.edges),
        (
            "node property",
            small.declared.node_properties,
            large.declared.node_properties,
        ),
        (
            "edge property",
            small.declared.edge_properties,
            large.declared.edge_properties,
        ),
    ] {
        assert!(
            small > 0 && large >= 8 * small,
            "{what} payload {small} -> {large}"
        );
    }

    for size in [&small, &large] {
        let nodes = size.nodes;
        let declared = &size.declared;
        // A scan that returns every value reads every fragment holding them.
        assert!(
            size.node_scan_read >= declared.node_properties,
            "nodes={nodes}: the node property scan read {} bytes of {} declared",
            size.node_scan_read,
            declared.node_properties
        );
        assert!(
            size.edge_scan_read >= declared.edge_properties,
            "nodes={nodes}: the edge property scan read {} bytes of {} declared",
            size.edge_scan_read,
            declared.edge_properties
        );
        // A complete export reads the whole payload the manifest declares. The
        // bound is the manifest's, not the receipt's re-encoded size, so a
        // codec change cannot move it.
        let payload =
            declared.nodes + declared.edges + declared.node_properties + declared.edge_properties;
        assert!(
            size.export_read >= payload,
            "nodes={nodes}: the export read {} bytes of {payload} declared payload",
            size.export_read
        );
        // And it carries that payload, reading what it writes: the package
        // holds the generation's objects byte for byte, so the receipt counts
        // at least the declared payload.
        assert!(
            size.export_payload >= payload,
            "nodes={nodes}: the export wrote {} payload bytes of {payload} declared",
            size.export_payload
        );
        assert!(
            size.export_read >= size.export_payload,
            "nodes={nodes}: the export read {} bytes for {} payload bytes written",
            size.export_read,
            size.export_payload
        );
    }
}
