//! A property-match node selector resolves by an anchored lookup, not by
//! scanning the whole topology or property route (#1989).
//!
//! The selector once scanned every node and was capped at 1,000,000 scanned
//! rows, so a graph above that size refused every `paths` call that named an
//! endpoint by property. The graph here is larger than that old cap.

use arrow::array::{Array, FixedSizeBinaryArray, ListArray};
use arrow::record_batch::RecordBatch;
use graphforge_api::{GraphForge, NodeSelector, PathAlgorithm, PathsOptions, PropValue};
use graphforge_storage::io_stats;

#[path = "support/bulk_fixture.rs"]
mod bulk_fixture;

use bulk_fixture::{fixture_node_name, fixture_node_uuid, generate_bulk_graph_with_properties};

/// More nodes than the retired 1,000,000-row selector scan cap.
const NODES: usize = 1_000_100;

fn by_name(index: usize) -> NodeSelector {
    NodeSelector::Match {
        label: "Entity".into(),
        property: "name".into(),
        value: PropValue::Str(fixture_node_name(index)),
    }
}

fn bfs() -> PathsOptions {
    PathsOptions {
        by: PathAlgorithm::Bfs,
        directed: true,
        ..PathsOptions::default()
    }
}

fn path_uuids(batch: &RecordBatch) -> Vec<[u8; 16]> {
    assert_eq!(batch.num_rows(), 1);
    let paths = batch
        .column_by_name("path")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = paths.value(0);
    let values = values
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    (0..values.len())
        .map(|index| values.value(index).try_into().unwrap())
        .collect()
}

#[test]
fn match_selector_resolves_on_a_graph_over_one_million_nodes_without_a_whole_graph_read() {
    let directory = tempfile::tempdir().unwrap();
    // A ring: every node links to its successor, so node 3 is three hops from node 0.
    generate_bulk_graph_with_properties(directory.path(), NODES, 1, false);
    let graph = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
    let expected = [0, 1, 2, 3].map(|node| *fixture_node_uuid(node).as_bytes());

    // `paths` itself reads the topology once for its adjacency view. The
    // baseline names the endpoints by UUID, which costs no selector lookup, so
    // any read beyond it belongs to selector resolution.
    let _capture = io_stats::CaptureScope::install();
    io_stats::reset();
    let by_uuid = graph
        .paths(
            &NodeSelector::Uuid(fixture_node_uuid(0)),
            Some(&NodeSelector::Uuid(fixture_node_uuid(3))),
            bfs(),
        )
        .unwrap();
    let baseline = io_stats::snapshot().expect("requested I/O statistics");
    assert_eq!(path_uuids(&by_uuid), expected);

    io_stats::reset();
    let by_match = graph
        .paths(&by_name(0), Some(&by_name(3)), bfs())
        .unwrap_or_else(|error| panic!("match selectors on {NODES} nodes: {error:?}"));
    let io = io_stats::snapshot().expect("requested I/O statistics");
    assert_eq!(path_uuids(&by_match), expected);
    assert_eq!(
        (io.node_full_reads, io.node_full_rows),
        (baseline.node_full_reads, baseline.node_full_rows),
        "selector resolution read the topology: {io:#?} vs {baseline:#?}"
    );

    // A name no node carries still refuses with the typed validation error.
    assert!(matches!(
        graph.paths(&by_name(NODES + 7), Some(&by_name(3)), bfs()),
        Err(graphforge_api::GfError::Validation(message)) if message == "node selector matched no nodes"
    ));
}
