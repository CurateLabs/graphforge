//! #1518: the generic (unordered-frontier) two-hop — the exact S17/S18 query
//! shape — through the real facade on a graph whose derived adjacency index is
//! **multi-shard**. An explicit 16 MiB resource policy derives
//! `shard_max_edges = memory_budget_bytes / 192` (about 87k edges), well below
//! this fixture's edge count, so the serving CSR is genuinely multi-shard.
//! The bounded-decode guarantee itself is proven at the storage layer
//! (`alternating_shard_frontier_decodes_each_shard_once`) and the provider
//! layer (`alternating_frontier_serves_correct_rows_over_multi_shard_index`);
//! the ignored S18-shaped measurement documents the wall-clock win.

use std::sync::Arc;

use arrow::array::{ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, StringArray};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy, GraphForge,
    GraphForgeOptions, ResourcePolicyMode,
};

const NODES: usize = 65_536;
/// Two distinct neighbours per node: `+1` and `+NODES/2` (no parallel edges).
/// The half-ring stride puts every source's two neighbours on opposite sides
/// of the surrogate midpoint, so the hop-two frontier alternates shards.
const EDGE_ROWS: usize = NODES * 2;

fn node_uuid(index: usize) -> graphforge_core::uuid::Uuid {
    graphforge_core::uuid::Uuid::from_u128(
        0x2000_0000_0000_0000_0000_0000_0000_0000 | index as u128 + 1,
    )
}

fn edge_uuid(index: usize) -> graphforge_core::uuid::Uuid {
    graphforge_core::uuid::Uuid::from_u128(
        0x2100_0000_0000_0000_0000_0000_0000_0000 | index as u128 + 1,
    )
}

fn append_identities(
    builder: &mut FixedSizeBinaryBuilder,
    ids: impl Iterator<Item = graphforge_core::uuid::Uuid>,
) {
    for id in ids {
        builder.append_value(id.as_bytes()).unwrap();
    }
}

/// Node `i` links to `i+1` and `i+NODES/2`.
fn neighbour(node: usize) -> [usize; 2] {
    [(node + 1) % NODES, (node + NODES / 2) % NODES]
}

#[test]
fn generic_two_hop_matches_expected_over_multi_shard_csr() {
    let project = tempfile::TempDir::new().unwrap();
    let forge = GraphForge::new_with_options(
        Some(project.path().to_str().expect("project path is UTF-8")),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                mode: ResourcePolicyMode::Explicit,
                memory_budget_bytes: Some(16 * 1024 * 1024),
                tokio_worker_threads: Some(1),
                compute_threads: Some(1),
                io_concurrency: Some(1),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .unwrap();
    let mut construction = forge.begin_graph_construction(Default::default()).unwrap();
    for start in (0..NODES).step_by(1_024) {
        let mut identities = FixedSizeBinaryBuilder::with_capacity(1_024, 16);
        append_identities(&mut identities, (start..start + 1_024).map(node_uuid));
        let batch = RecordBatch::try_new(
            CONSTRUCTION_NODE_SCHEMA.clone(),
            vec![
                Arc::new(identities.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["Entity"; 1_024])),
            ],
        )
        .unwrap();
        construction
            .append_nodes(&format!("nodes-{start}"), &batch)
            .unwrap();
    }
    for start in (0..EDGE_ROWS).step_by(8_192) {
        let rows = 8_192.min(EDGE_ROWS - start);
        let mut identities = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        for edge in start..start + rows {
            let source = edge / 2;
            let offset = if edge % 2 == 0 { 1 } else { NODES / 2 };
            append_identities(&mut identities, std::iter::once(edge_uuid(edge)));
            append_identities(&mut sources, std::iter::once(node_uuid(source)));
            append_identities(
                &mut targets,
                std::iter::once(node_uuid((source + offset) % NODES)),
            );
        }
        let batch = RecordBatch::try_new(
            CONSTRUCTION_EDGE_SCHEMA.clone(),
            vec![
                Arc::new(identities.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["LINK"; rows])),
                Arc::new(sources.finish()),
                Arc::new(targets.finish()),
            ],
        )
        .unwrap();
        construction
            .append_edges(&format!("edges-{start}"), &batch)
            .unwrap();
    }
    construction.seal_and_publish().unwrap();
    // The policy-derived shard cap (about 87k edges) is far below EDGE_ROWS,
    // so the derived adjacency CSR for LINK is necessarily multi-shard.
    forge.index_adjacency().unwrap();
    let inspection = forge.inspect_adjacency().unwrap();
    assert_eq!(
        inspection.state,
        graphforge_storage::adjacency::AdjacencyFreshnessState::Current
    );

    let result = forge
        .execute("MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id ORDER BY id LIMIT 1000")
        .unwrap();
    let mut actual: Vec<u128> = result
        .batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| u128::from_be_bytes(values.value(row).try_into().unwrap()))
                .collect::<Vec<_>>()
        })
        .collect();
    actual.sort_unstable();

    let mut expected: Vec<u128> = (0..NODES)
        .flat_map(|source| {
            neighbour(source)
                .iter()
                .flat_map(|middle| neighbour(*middle))
                .map(|destination| node_uuid(destination).as_u128())
                .collect::<Vec<_>>()
        })
        .collect();
    expected.sort_unstable();
    expected.truncate(1000);
    assert_eq!(actual, expected);
}
