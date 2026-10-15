//! Destination topology admission exercised through the real Rust facade.

use arrow::array::{TimestampMicrosecondArray, UInt64Array};
use graphforge_api::{ExecutionResourcePolicy, GraphForge, GraphForgeOptions};
use graphforge_core::OntologyMode;
use graphforge_core::uuid::new_v7;
use graphforge_exec::demand;
use graphforge_storage::GraphWriter;

#[path = "support/project_fixture.rs"]
mod project_fixture;

#[test]
fn admitted_destination_file_hop_preserves_exact_facade_rows_and_schema() {
    const NODES: usize = 16;
    const TS: i64 = 1_700_000_000_000_000;
    let project = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let mut writer = GraphWriter::open_at(workspace.path(), OntologyMode::Exploratory, TS).unwrap();
    let uuids = (0..NODES).map(|_| new_v7()).collect::<Vec<_>>();
    let first = graphforge_value::EntityTypeId::decode(0).unwrap();
    let second = graphforge_value::EntityTypeId::decode(1).unwrap();
    let ids = uuids
        .iter()
        .enumerate()
        .map(|(index, uuid)| {
            let labels = if index % 2 == 0 {
                vec![first, second]
            } else {
                vec![first]
            };
            writer.create_node_with_labels(*uuid, &labels).unwrap()
        })
        .collect::<Vec<_>>();
    for source in 0..NODES {
        for offset in [1, 2] {
            writer
                .create_edge(
                    new_v7(),
                    "LINK",
                    &uuids[source],
                    &uuids[(source + offset) % NODES],
                )
                .unwrap();
        }
    }
    writer.flush().unwrap();
    drop(writer);
    graphforge_storage::adjacency::build_adjacency_index(workspace.path(), TS).unwrap();
    project_fixture::publish_graph_workspace(project.path(), workspace.path());
    let forge = GraphForge::new_with_options(
        Some(project.path().to_str().unwrap()),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                memory_budget_bytes: Some(16 * 1024 * 1024),
                target_partitions: Some(1),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .unwrap();
    // created_at cannot be synthesized from the label index or ordinal UUID
    // authority: the physical expansion must read its destination node file.
    let query = "MATCH (a)-[:LINK]->(b) RETURN a.node_id AS source, \
                 b.node_id AS destination, b.created_at AS created";
    assert!(forge.explain(query).unwrap().contains("ExpandExec"));
    let (result, observed) = demand::capture(|| forge.execute(query));
    let result = result.unwrap();
    let mut actual = Vec::new();
    for batch in &result.batches {
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["source", "destination", "created"]
        );
        let sources = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let destinations = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let timestamps = batch
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            actual.push((
                sources.value(row),
                destinations.value(row),
                timestamps.value(row),
            ));
        }
    }
    actual.sort_unstable();
    let mut expected = (0..NODES)
        .flat_map(|source| {
            let ids = &ids;
            [1, 2]
                .into_iter()
                .map(move |offset| (ids[source], ids[(source + offset) % NODES], TS))
        })
        .collect::<Vec<_>>();
    expected.sort_unstable();
    assert_eq!(actual, expected);
    assert!(
        observed
            .hops
            .values()
            .map(|hop| hop.node_reads_started)
            .sum::<u64>()
            > 0,
        "{observed:#?}"
    );
    assert_eq!(
        observed
            .hops
            .values()
            .map(|hop| hop.edge_reads_started)
            .sum::<u64>(),
        0,
        "unused relationships must preserve edge-read avoidance"
    );
    assert!(
        observed.memory_reserved_after
            <= observed
                .memory_reserved_before
                .saturating_add(observed.returned_batch_bytes),
        "{observed:#?}"
    );
}

#[test]
fn carried_concat_value_survives_an_admitted_destination_hop() {
    const NODES: usize = 16;
    const TS: i64 = 1_700_000_000_000_000;
    let project = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let mut writer = GraphWriter::open_at(workspace.path(), OntologyMode::Exploratory, TS).unwrap();
    let uuids = (0..NODES).map(|_| new_v7()).collect::<Vec<_>>();
    let first = graphforge_value::EntityTypeId::decode(0).unwrap();
    let second = graphforge_value::EntityTypeId::decode(1).unwrap();
    let ids = uuids
        .iter()
        .enumerate()
        .map(|(index, uuid)| {
            let labels = if index % 2 == 0 {
                vec![first, second]
            } else {
                vec![first]
            };
            writer.create_node_with_labels(*uuid, &labels).unwrap()
        })
        .collect::<Vec<_>>();
    for source in 0..NODES {
        for offset in [1, 2] {
            writer
                .create_edge(
                    new_v7(),
                    "LINK",
                    &uuids[source],
                    &uuids[(source + offset) % NODES],
                )
                .unwrap();
        }
    }
    writer.flush().unwrap();
    drop(writer);
    graphforge_storage::adjacency::build_adjacency_index(workspace.path(), TS).unwrap();
    project_fixture::publish_graph_workspace(project.path(), workspace.path());
    let forge = GraphForge::new_with_options(
        Some(project.path().to_str().unwrap()),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                memory_budget_bytes: Some(16 * 1024 * 1024),
                target_partitions: Some(1),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .unwrap();
    // CONCAT's null coercion is chosen by the real expression planner. Carry
    // that value through Expand while created_at forces the real node reader.
    let query = "MATCH (a) WITH a, concat(toString(a.node_id), null) AS carried \
                 MATCH (a)-[:LINK]->(b) \
                 RETURN carried, b.node_id AS destination, b.created_at AS created";
    assert!(forge.explain(query).unwrap().contains("ExpandExec"));
    let (result, observed) = demand::capture(|| forge.execute(query));
    let result = result.unwrap();
    let mut actual = Vec::new();
    for batch in &result.batches {
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["carried", "destination", "created"]
        );
        let destinations = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let timestamps = batch
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(!batch.column(0).is_null(row));
            actual.push((
                arrow::util::display::array_value_to_string(batch.column(0).as_ref(), row).unwrap(),
                destinations.value(row),
                timestamps.value(row),
            ));
        }
    }
    actual.sort_unstable();
    let mut expected = (0..NODES)
        .flat_map(|source| {
            let ids = &ids;
            [1, 2]
                .into_iter()
                .map(move |offset| (ids[source].to_string(), ids[(source + offset) % NODES], TS))
        })
        .collect::<Vec<_>>();
    expected.sort_unstable();
    assert_eq!(actual, expected);
    assert!(
        observed
            .hops
            .values()
            .map(|hop| hop.node_reads_started)
            .sum::<u64>()
            > 0,
        "{observed:#?}"
    );
    assert_eq!(
        observed
            .hops
            .values()
            .map(|hop| hop.edge_reads_started)
            .sum::<u64>(),
        0
    );
    assert!(
        observed.memory_reserved_after
            <= observed
                .memory_reserved_before
                .saturating_add(observed.returned_batch_bytes),
        "{observed:#?}"
    );
}
