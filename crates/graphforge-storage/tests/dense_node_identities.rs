//! The dense node-identity read against real multi-file node tables.

use std::collections::HashSet;

use arrow::array::{Array, FixedSizeBinaryArray, UInt64Array};
use graphforge_core::OntologyMode;
use graphforge_core::uuid::new_v7;
use graphforge_storage::{
    DenseNodeIdentityError, GraphWriter, TopologyFiles, delete_nodes, read_dense_node_identities,
    read_nodes_from_files,
};
use graphforge_value::EntityTypeId;
use tempfile::TempDir;

const TS: i64 = 1_700_000_000_000_000;
const SESSIONS: usize = 3;
const NODES_PER_SESSION: usize = 4;

/// One writer session per node file; returns the UUIDs in creation order.
fn write_node_table(root: &std::path::Path) -> Vec<[u8; 16]> {
    let mut created = Vec::new();
    for session in 0..SESSIONS {
        let mut writer =
            GraphWriter::open_at(root, OntologyMode::Exploratory, TS + session as i64).unwrap();
        for _ in 0..NODES_PER_SESSION {
            let uuid = new_v7();
            writer
                .create_node(uuid, EntityTypeId::decode(0).unwrap())
                .unwrap();
            created.push(uuid.into_bytes());
        }
        writer.flush().unwrap();
    }
    created
}

/// `(node_id, node_uuid)` pairs read through the full-schema reader.
fn oracle(files: &TopologyFiles) -> Vec<(u64, [u8; 16])> {
    let mut rows = Vec::new();
    for batch in read_nodes_from_files(files).unwrap() {
        let ids = batch
            .column_by_name("node_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let uuids = batch
            .column_by_name("node_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), uuids.value(row).try_into().unwrap()));
        }
    }
    rows
}

#[test]
fn dense_identities_match_every_ordinal_of_a_multi_file_table() {
    let root = TempDir::new().unwrap();
    let created = write_node_table(root.path());
    let files = TopologyFiles::discover_legacy(root.path()).unwrap();
    assert_eq!(
        files.node_fragments().len(),
        SESSIONS,
        "one file per session"
    );

    let identities = read_dense_node_identities(&files).unwrap();
    assert_eq!(identities.uuids.len(), SESSIONS * NODES_PER_SESSION);
    let expected = oracle(&files);
    for (ordinal, (node_id, uuid)) in expected.iter().enumerate() {
        assert_eq!(*node_id, identities.first_node_id + ordinal as u64);
        assert_eq!(&identities.uuids[ordinal], uuid, "ordinal {ordinal}");
        assert_eq!(identities.uuid_of(*node_id), Some(uuid));
    }
    // Ids are assigned in creation order, so the ordinals are the created order.
    assert_eq!(identities.uuids, created);
    assert_eq!(identities.uuid_of(identities.first_node_id + 99), None);
    assert_eq!(
        identities.uuid_of(identities.first_node_id.wrapping_sub(1)),
        None
    );
}

#[test]
fn dense_identities_of_an_empty_table_are_empty() {
    let root = TempDir::new().unwrap();
    let files = TopologyFiles::discover_legacy(root.path()).unwrap();
    let identities = read_dense_node_identities(&files).unwrap();
    assert!(identities.uuids.is_empty());
    assert_eq!(identities.uuid_of(1), None);
}

#[test]
fn dense_identities_refuse_a_table_with_deleted_nodes() {
    let root = TempDir::new().unwrap();
    let created = write_node_table(root.path());
    let files = TopologyFiles::discover_legacy(root.path()).unwrap();
    let before = oracle(&files);

    // Deleting the second node leaves a gap inside the first file.
    assert_eq!(
        delete_nodes(root.path(), &HashSet::from([created[1]])).unwrap(),
        1
    );
    let files = TopologyFiles::discover_legacy(root.path()).unwrap();
    let remaining = oracle(&files);
    assert_eq!(remaining.len(), before.len() - 1);
    match read_dense_node_identities(&files) {
        Err(DenseNodeIdentityError::NotDense {
            ordinal,
            expected_node_id,
            found_node_id,
        }) => {
            assert_eq!(ordinal, 1);
            assert_eq!(expected_node_id, before[1].0);
            assert_eq!(found_node_id, before[2].0);
        }
        other => panic!("a gapped table must be refused as not dense, got {other:?}"),
    }
}

#[test]
fn dense_identities_refuse_a_gap_between_files() {
    let root = TempDir::new().unwrap();
    let created = write_node_table(root.path());
    let first_of_second_file = created[NODES_PER_SESSION];
    assert_eq!(
        delete_nodes(root.path(), &HashSet::from([first_of_second_file])).unwrap(),
        1
    );
    let files = TopologyFiles::discover_legacy(root.path()).unwrap();
    assert_eq!(files.node_fragments().len(), SESSIONS);
    let error = read_dense_node_identities(&files).unwrap_err();
    assert!(
        matches!(
            error,
            DenseNodeIdentityError::NotDense { ordinal, .. } if ordinal == NODES_PER_SESSION as u64
        ),
        "{error:?}"
    );
    assert!(error.to_string().contains("not dense"));
}
