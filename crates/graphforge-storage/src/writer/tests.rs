use super::*;
use crate::writer::Arc;
use crate::writer::DataType;
use crate::writer::ENDPOINT_ENTRY_CHARGE;
use crate::writer::EntityTypeId;
use crate::writer::Field;
use crate::writer::FixedSizeBinaryArray;
use crate::writer::GfError;
use crate::writer::GraphWriter;
use crate::writer::GraphWriterLimits;
use crate::writer::HashMap;
use crate::writer::HashSet;
use crate::writer::IrLiteral;
use crate::writer::NODE_ROW_CHARGE;
use crate::writer::NODE_SCRATCH_CHARGE;
use crate::writer::OntologyMode;
use crate::writer::Path;
use crate::writer::RecordBatch;
use crate::writer::RewriteBatch;
use crate::writer::SURROGATE_TAILS_FILE;
use crate::writer::Schema;
use crate::writer::TOPOLOGY_NODES_SCHEMA;
use crate::writer::TimestampMicrosecondArray;
use crate::writer::TopologyWriteWork;
use crate::writer::UInt32Array;
use crate::writer::UInt64Array;
use crate::writer::Uuid;
use crate::writer::decode_edge_property_rows;
use crate::writer::fs;
use crate::writer::read_node_property_rows;
use crate::writer::remove_node_properties;
use crate::writer::set_node_properties;
use crate::writer::size_of;
use crate::writer::to_bytes;
use crate::writer::uuid_field;
use graphforge_core::uuid::new_v7;
use std::fs::File;
use tempfile::TempDir;

pub(super) const TS: i64 = 1_700_000_000_000_000;

fn test_inventory(dir: &Path) -> crate::AuthenticatedPropertyInventory {
    let (files, _) = crate::capture_graph_files(dir).unwrap();
    crate::AuthenticatedPropertyInventory::from_inventory_at_root(dir, files, None).unwrap()
}

#[test]
fn create_node_persists_complete_label_set_and_primary_label() {
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    w.create_node_with_labels(
        new_v7(),
        &[
            EntityTypeId::decode(4).unwrap(),
            EntityTypeId::decode(9).unwrap(),
        ],
    )
    .unwrap();
    w.flush().unwrap();

    let nodes = crate::catalog::read_nodes(dir.path()).unwrap();
    let batch = &nodes[0];
    let primary = batch
        .column_by_name("type_id")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    assert_eq!(primary.value(0), 4);
    let sets = batch
        .column_by_name("type_ids")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .unwrap();
    let labels = sets.value(0);
    let labels = labels.as_any().downcast_ref::<UInt32Array>().unwrap();
    assert_eq!(labels.values(), &[4, 9]);
}

#[test]
fn surrogate_ids_are_monotonic_from_one() {
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    assert_eq!(
        w.create_node(new_v7(), EntityTypeId::decode(0).unwrap())
            .unwrap(),
        1
    );
    assert_eq!(
        w.create_node(new_v7(), EntityTypeId::decode(0).unwrap())
            .unwrap(),
        2
    );
    assert_eq!(
        w.create_node(new_v7(), EntityTypeId::decode(0).unwrap())
            .unwrap(),
        3
    );
}

#[test]
fn reopened_property_patch_seals_a_complete_authenticated_snapshot() {
    let dir = TempDir::new().unwrap();
    let node = new_v7();
    let mut first = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    first
        .create_node(node, EntityTypeId::decode(0).unwrap())
        .unwrap();
    first
        .set_properties(
            &node,
            None,
            HashMap::from([
                ("name".to_owned(), IrLiteral::Str("kept".to_owned())),
                ("ts".to_owned(), IrLiteral::Int(7)),
            ]),
        )
        .unwrap();
    first.flush().unwrap();

    let mut reopened = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS + 1).unwrap();
    reopened
        .set_properties(
            &node,
            None,
            HashMap::from([("patched".to_owned(), IrLiteral::Bool(true))]),
        )
        .unwrap();
    reopened.flush().unwrap();

    let (captured, _) = crate::capture_graph_files(dir.path()).unwrap();
    let inventory =
        crate::AuthenticatedPropertyInventory::from_inventory_at_root(dir.path(), captured, None)
            .unwrap();
    let scratch = TempDir::new().unwrap();
    let mut rows = Vec::new();
    inventory
        .visit_route(
            crate::PropertyRouteKind::Node,
            "_untyped",
            scratch.path(),
            crate::PropertyOverlayLimits::default(),
            |row| {
                rows.push(row);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].uuid, node.into_bytes());
    assert_eq!(rows[0].values["name"], IrLiteral::Str("kept".into()));
    assert_eq!(rows[0].values["ts"], IrLiteral::Int(7));
    assert_eq!(rows[0].values["patched"], IrLiteral::Bool(true));
}

#[test]
fn reopen_recovers_surrogate_tails_without_full_topology_reads() {
    const CHILD: &str = "GRAPHFORGE_SURROGATE_TAIL_REOPEN_IO_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("writer::tests::reopen_recovers_surrogate_tails_without_full_topology_reads")
            .arg("--nocapture")
            .env(CHILD, "1")
            .status()
            .unwrap();
        assert!(
            status.success(),
            "isolated surrogate-tail reopen I/O proof failed"
        );
        return;
    }

    let dir = TempDir::new().unwrap();
    let first_node = new_v7();
    let second_node = new_v7();
    let mut first = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    assert_eq!(
        first
            .create_node(first_node, EntityTypeId::decode(0).unwrap())
            .unwrap(),
        1
    );
    assert_eq!(
        first
            .create_node(second_node, EntityTypeId::decode(0).unwrap())
            .unwrap(),
        2
    );
    assert_eq!(
        first
            .create_edge(new_v7(), "KNOWS", &first_node, &second_node)
            .unwrap(),
        1
    );
    first.flush().unwrap();
    assert!(dir.path().join(SURROGATE_TAILS_FILE).is_file());

    let _measurement = crate::io_stats::test_measurement_guard();
    crate::io_stats::reset();
    let mut reopened = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    assert_eq!(
        reopened
            .create_node(new_v7(), EntityTypeId::decode(0).unwrap())
            .unwrap(),
        3
    );
    reopened.register_existing_node(first_node, 1).unwrap();
    reopened.register_existing_node(second_node, 2).unwrap();
    assert_eq!(
        reopened
            .create_edge(new_v7(), "KNOWS", &first_node, &second_node)
            .unwrap(),
        2
    );
    let io = crate::io_stats::snapshot();
    assert_eq!(
        io.node_full_reads, 0,
        "writer reopen must use bounded tails"
    );
    assert_eq!(
        io.edge_full_reads, 0,
        "writer reopen must use bounded tails"
    );
}

#[test]
fn authenticated_endpoint_registration_decodes_zero_topology_rows() {
    const CHILD: &str = "GRAPHFORGE_ENDPOINT_REGISTRATION_IO_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("writer::tests::authenticated_endpoint_registration_decodes_zero_topology_rows")
            .arg("--nocapture")
            .env(CHILD, "1")
            .status()
            .unwrap();
        assert!(
            status.success(),
            "isolated endpoint-registration I/O proof failed"
        );
        return;
    }

    let dir = TempDir::new().unwrap();
    let left = new_v7();
    let right = new_v7();
    let mut seed = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    seed.create_node(left, EntityTypeId::decode(0).unwrap())
        .unwrap();
    seed.create_node(right, EntityTypeId::decode(0).unwrap())
        .unwrap();
    seed.flush().unwrap();
    crate::rebuild_uuid_membership_indexes(
        dir.path(),
        crate::UuidIndexBuildLimits {
            scan_batch_rows: 1,
            run_records: 1,
            merge_fan_in: 2,
        },
    )
    .unwrap();

    crate::io_stats::reset();
    let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS + 1).unwrap();
    let metrics = writer.register_existing_endpoints(&[left, right]).unwrap();
    assert_eq!(metrics.found, 2);
    assert_eq!(metrics.per_record_seeks, 0);
    assert_eq!(metrics.identity_blocks_read, 1);
    assert_eq!(metrics.surrogate_blocks_read, 1);
    assert_eq!(writer.topology_write_work().uuid_per_record_seeks, 0);
    assert_eq!(writer.topology_write_work().uuid_block_seeks, 2);
    writer
        .create_edge(new_v7(), "KNOWS", &left, &right)
        .unwrap();
    let io = crate::io_stats::snapshot();
    assert_eq!(io.node_full_reads, 0);
    assert_eq!(io.node_filtered_reads, 0);
}

#[test]
fn label_only_topology_commits_keep_endpoint_authority_current() {
    let dir = TempDir::new().unwrap();
    let left = new_v7();
    let right = new_v7();
    let mut seed = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    seed.create_node(left, EntityTypeId::decode(0).unwrap())
        .unwrap();
    seed.create_node(right, EntityTypeId::decode(0).unwrap())
        .unwrap();
    seed.flush().unwrap();

    let mut additions = HashMap::new();
    additions.insert(
        to_bytes(&left),
        HashSet::from([EntityTypeId::decode(7).unwrap()]),
    );
    let mut add_labels =
        GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS + 1).unwrap();
    let mut staged = RewriteBatch::new();
    crate::stage_mutate_node_labels(
        &mut staged,
        dir.path(),
        &additions,
        &HashMap::<[u8; 16], HashSet<EntityTypeId>>::new(),
    )
    .unwrap();
    assert_eq!(
        add_labels
            .commit_topology_aware_with_uuid_index(staged, Vec::new(), Vec::new())
            .unwrap(),
        Some(2)
    );
    assert_eq!(
        add_labels
            .topology_write_work()
            .uuid_prior_topology_rows_decoded,
        0
    );
    assert!(crate::uuid_membership_index_is_fresh(dir.path()).unwrap());
    let mut after_add =
        GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS + 2).unwrap();
    assert_eq!(
        after_add
            .register_existing_endpoints(&[left, right])
            .unwrap()
            .found,
        2
    );

    let mut removals = HashMap::new();
    removals.insert(
        to_bytes(&left),
        HashSet::from([EntityTypeId::decode(7).unwrap()]),
    );
    let mut remove_labels =
        GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS + 3).unwrap();
    let mut staged = RewriteBatch::new();
    crate::stage_mutate_node_labels(
        &mut staged,
        dir.path(),
        &HashMap::<[u8; 16], HashSet<EntityTypeId>>::new(),
        &removals,
    )
    .unwrap();
    assert_eq!(
        remove_labels
            .commit_topology_aware_with_uuid_index(staged, Vec::new(), Vec::new())
            .unwrap(),
        Some(3)
    );
    assert_eq!(
        remove_labels
            .topology_write_work()
            .uuid_prior_topology_rows_decoded,
        0
    );
    assert!(crate::uuid_membership_index_is_fresh(dir.path()).unwrap());
    let mut after_remove =
        GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS + 4).unwrap();
    assert_eq!(
        after_remove
            .register_existing_endpoints(&[left, right])
            .unwrap()
            .found,
        2
    );
}

#[test]
fn edge_appends_create_immutable_shards_without_prior_row_replay() {
    let dir = TempDir::new().unwrap();
    let left = new_v7();
    let right = new_v7();
    let mut first = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    first
        .create_node(left, EntityTypeId::decode(0).unwrap())
        .unwrap();
    first
        .create_node(right, EntityTypeId::decode(0).unwrap())
        .unwrap();
    first.create_edge(new_v7(), "KNOWS", &left, &right).unwrap();
    first.flush().unwrap();

    let mut second = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    second.register_existing_node(left, 1).unwrap();
    second.register_existing_node(right, 2).unwrap();
    second
        .create_edge(new_v7(), "KNOWS", &right, &left)
        .unwrap();
    second.flush().unwrap();

    let fragments = crate::mutator::edge_parquet_files(
        dir.path(),
        Some(&crate::route_component::component("KNOWS")),
    )
    .unwrap();
    assert_eq!(fragments.len(), 2);
    assert!(fragments.iter().any(|(_, path)| {
        path.parent().and_then(Path::file_name)
            == Some(std::ffi::OsStr::new(&crate::route_component::component(
                "KNOWS",
            )))
    }));
    let work = second.topology_write_work();
    assert_eq!(work.existing_rows_rewritten, 0);
    assert_eq!(work.new_rows_written, 1);
    assert_eq!(work.input_rows, 1);
    assert_eq!(work.prior_rows_decoded, 0);
    assert_eq!(work.rows_encoded, 1);
    assert_eq!(work.shard_count, 1);
    assert!(work.output_bytes > 0);
    let rows = crate::catalog::read_edges_from_inventory(
        &test_inventory(dir.path()),
        "KNOWS",
        OntologyMode::Strict,
    )
    .unwrap()
    .into_iter()
    .map(|batch| batch.num_rows())
    .sum::<usize>();
    assert_eq!(rows, 2, "ordinary direct reader must union all fragments");
}

#[test]
fn node_appends_create_immutable_shards_without_prior_row_replay() {
    let dir = TempDir::new().unwrap();
    let mut first = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    first
        .create_node(new_v7(), EntityTypeId::decode(0).unwrap())
        .unwrap();
    first.flush().unwrap();

    let mut second = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    second
        .create_node(new_v7(), EntityTypeId::decode(0).unwrap())
        .unwrap();
    second.flush().unwrap();

    let fragments = crate::mutator::node_parquet_files(dir.path()).unwrap();
    assert_eq!(fragments.len(), 2);
    assert_eq!(second.topology_write_work().existing_rows_rewritten, 0);
    assert_eq!(second.topology_write_work().new_rows_written, 1);
    let rows = crate::catalog::read_nodes(dir.path())
        .unwrap()
        .into_iter()
        .map(|batch| batch.num_rows())
        .sum::<usize>();
    assert_eq!(rows, 2);
    let mut third = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    assert_eq!(
        third
            .create_node(new_v7(), EntityTypeId::decode(0).unwrap())
            .unwrap(),
        3
    );
}

#[test]
fn node_append_rejects_an_existing_surrogate_range_shard() {
    let dir = TempDir::new().unwrap();
    let mut first = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    first
        .create_node(new_v7(), EntityTypeId::decode(0).unwrap())
        .unwrap();
    first.flush().unwrap();

    let mut second = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS + 1).unwrap();
    second
        .create_node(new_v7(), EntityTypeId::decode(0).unwrap())
        .unwrap();
    let collision = dir
        .path()
        .join("topology/nodes/00000000000000000002-00000000000000000002.parquet");
    fs::create_dir_all(collision.parent().unwrap()).unwrap();
    fs::write(&collision, b"planted collision").unwrap();

    let error = second.flush().unwrap_err().to_string();
    assert!(error.contains("node shard surrogate range already exists"));
    assert_eq!(fs::read(collision).unwrap(), b"planted collision");
}

#[test]
fn property_appends_create_immutable_shards_and_ordinary_reader_unions_them() {
    let dir = TempDir::new().unwrap();
    let first_uuid = new_v7();
    let mut first = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    first
        .create_node(first_uuid, EntityTypeId::decode(0).unwrap())
        .unwrap();
    first
        .set_properties(
            &first_uuid,
            None,
            HashMap::from([("age".to_owned(), IrLiteral::Int(30))]),
        )
        .unwrap();
    first.flush().unwrap();

    let second_uuid = new_v7();
    let mut second = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS + 1).unwrap();
    second
        .create_node(second_uuid, EntityTypeId::decode(0).unwrap())
        .unwrap();
    second
        .set_properties(
            &second_uuid,
            None,
            HashMap::from([("name".to_owned(), IrLiteral::Str("Ada".into()))]),
        )
        .unwrap();
    second.flush().unwrap();

    let fragments = crate::mutator::property_parquet_files(
        dir.path(),
        "properties",
        &crate::route_component::component("_untyped"),
    )
    .unwrap();
    assert_eq!(fragments.len(), 2);
    let rows = read_node_property_rows(dir.path(), "_untyped").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[&to_bytes(&first_uuid)]["age"], IrLiteral::Int(30));
    assert_eq!(
        rows[&to_bytes(&second_uuid)]["name"],
        IrLiteral::Str("Ada".into())
    );
    set_node_properties(
        dir.path(),
        "_untyped",
        &HashMap::from([(
            to_bytes(&second_uuid),
            HashMap::from([("name".to_owned(), IrLiteral::Str("Grace".into()))]),
        )]),
    )
    .unwrap();
    remove_node_properties(
        dir.path(),
        "_untyped",
        &HashMap::from([(to_bytes(&first_uuid), HashSet::from(["age".to_owned()]))]),
    )
    .unwrap();
    let mutated = read_node_property_rows(dir.path(), "_untyped").unwrap();
    assert!(mutated[&to_bytes(&first_uuid)].is_empty());
    assert_eq!(
        mutated[&to_bytes(&second_uuid)]["name"],
        IrLiteral::Str("Grace".into())
    );
    let work = second.topology_write_work();
    assert_eq!(work.prior_rows_decoded, 0);
    assert_eq!(
        work.input_rows, 1,
        "property overlays are not topology work"
    );
    assert_eq!(work.rows_encoded, 1);
    assert_eq!(work.shard_count, 1);
}

#[test]
fn doubling_topology_input_is_linear_work_not_rewrite_work() {
    fn construct(rows: usize) -> TopologyWriteWork {
        let dir = TempDir::new().unwrap();
        let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
        for _ in 0..rows {
            writer
                .create_node(new_v7(), EntityTypeId::decode(0).unwrap())
                .unwrap();
        }
        writer.flush().unwrap();
        writer.topology_write_work()
    }

    let small = construct(128);
    let large = construct(256);
    assert_eq!(small.input_rows, 128);
    assert_eq!(large.input_rows, 256);
    assert_eq!(small.prior_rows_decoded, 0);
    assert_eq!(large.prior_rows_decoded, 0);
    assert_eq!(large.rows_encoded, small.rows_encoded * 2);
    assert_eq!(large.shard_count, small.shard_count);
    assert!(large.output_bytes <= small.output_bytes.saturating_mul(3));
}

#[test]
fn topology_budget_rejects_before_allocating_or_advancing_surrogates() {
    let dir = TempDir::new().unwrap();
    let required = NODE_ROW_CHARGE + ENDPOINT_ENTRY_CHARGE + size_of::<(Uuid, u64)>() + 4;
    let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS)
        .unwrap()
        .with_limits(GraphWriterLimits {
            max_buffered_topology_rows: 1,
            max_buffered_topology_bytes: required - 1,
            max_flush_scratch_bytes: usize::MAX,
        });

    assert!(
        writer
            .create_node(new_v7(), EntityTypeId::decode(0).unwrap())
            .is_err()
    );
    assert_eq!(writer.next_node_id, 1);
    assert!(writer.nodes.is_empty());
    assert!(writer.uuid_to_node_id.is_empty());
    assert_eq!(writer.charged_topology_bytes, 0);
    assert_eq!(writer.topology_work.peak_buffered_rows, 0);
}

#[test]
fn same_window_cross_kind_uuid_collisions_fail_before_state_mutation() {
    let dir = TempDir::new().unwrap();
    let left = new_v7();
    let right = new_v7();
    let collision = new_v7();
    let mut edge_first = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    edge_first
        .create_node(left, EntityTypeId::decode(0).unwrap())
        .unwrap();
    edge_first
        .create_node(right, EntityTypeId::decode(0).unwrap())
        .unwrap();
    edge_first
        .create_edge(collision, "KNOWS", &left, &right)
        .unwrap();
    let next_node = edge_first.next_node_id;
    assert!(
        edge_first
            .create_node(collision, EntityTypeId::decode(0).unwrap())
            .is_err()
    );
    assert_eq!(edge_first.next_node_id, next_node);
    assert_eq!(edge_first.nodes.len(), 2);

    let dir = TempDir::new().unwrap();
    let mut node_first = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    node_first
        .create_node(left, EntityTypeId::decode(0).unwrap())
        .unwrap();
    node_first
        .create_node(right, EntityTypeId::decode(0).unwrap())
        .unwrap();
    node_first
        .create_node(collision, EntityTypeId::decode(0).unwrap())
        .unwrap();
    let next_edge = node_first.next_edge_id;
    assert!(
        node_first
            .create_edge(collision, "KNOWS", &left, &right)
            .is_err()
    );
    assert_eq!(node_first.next_edge_id, next_edge);
    assert!(node_first.edges.values().all(Vec::is_empty));
}

#[test]
fn ordinary_writer_flush_keeps_v3_uuid_index_fresh_across_generations() {
    let dir = TempDir::new().unwrap();
    let first = new_v7();
    let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    writer
        .create_node(first, EntityTypeId::decode(0).unwrap())
        .unwrap();
    writer.flush().unwrap();
    assert!(
        crate::uuid_membership_index_is_fresh(dir.path()).unwrap(),
        "generation={} manifest={}",
        crate::read_topology_generation(dir.path()).unwrap(),
        std::fs::read_to_string(dir.path().join("topology/uuid-membership/manifest.json")).unwrap()
    );

    let second = new_v7();
    let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS + 1).unwrap();
    writer
        .create_node(second, EntityTypeId::decode(0).unwrap())
        .unwrap();
    writer.flush().unwrap();
    assert!(
        crate::uuid_membership_index_is_fresh(dir.path()).unwrap(),
        "generation={} manifest={}",
        crate::read_topology_generation(dir.path()).unwrap(),
        std::fs::read_to_string(dir.path().join("topology/uuid-membership/manifest.json")).unwrap()
    );
    let mut index = crate::UuidMembershipIndex::open(dir.path()).unwrap();
    assert_eq!(
        index.lookup_node_surrogates(&[first, second]).unwrap().0,
        [Some(1), Some(2)]
    );
}

#[test]
fn existing_endpoint_registration_propagates_budget_failure() {
    let dir = TempDir::new().unwrap();
    let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS)
        .unwrap()
        .with_limits(GraphWriterLimits {
            max_buffered_topology_rows: usize::MAX,
            max_buffered_topology_bytes: ENDPOINT_ENTRY_CHARGE - 1,
            max_flush_scratch_bytes: usize::MAX,
        });
    assert!(writer.register_existing_node(new_v7(), 7).is_err());
    assert!(writer.uuid_to_node_id.is_empty());
    assert_eq!(writer.charged_topology_bytes, 0);
}

#[test]
fn topology_budget_plateaus_across_committed_mixed_batches() {
    let dir = TempDir::new().unwrap();
    let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS)
        .unwrap()
        .with_limits(GraphWriterLimits {
            max_buffered_topology_rows: 3,
            max_buffered_topology_bytes: 16 * 1024,
            max_flush_scratch_bytes: 4 * 1024,
        });

    for _ in 0..8 {
        let left = new_v7();
        let right = new_v7();
        writer
            .create_node(left, EntityTypeId::decode(0).unwrap())
            .unwrap();
        writer
            .create_node(right, EntityTypeId::decode(0).unwrap())
            .unwrap();
        writer
            .create_edge(new_v7(), "KNOWS", &left, &right)
            .unwrap();
        assert!(writer.charged_topology_bytes <= writer.limits.max_buffered_topology_bytes);
        assert!(writer.flush_scratch_bytes <= writer.limits.max_flush_scratch_bytes);
        writer.flush().unwrap();
        writer.release_committed_topology_state();
        assert_eq!(writer.charged_topology_bytes, 0);
        assert_eq!(writer.buffered_topology_rows, 0);
        assert_eq!(writer.flush_scratch_bytes, 0);
    }
    assert_eq!(writer.topology_work.peak_buffered_rows, 3);
    assert!(writer.topology_work.peak_buffered_bytes <= 16 * 1024);
    assert!(writer.topology_work.peak_flush_scratch_bytes <= 4 * 1024);
    assert_eq!(writer.topology_work.new_rows_written, 24);
}

#[test]
fn topology_budget_accounts_for_cancel_and_failed_flush_retained_state() {
    let dir = TempDir::new().unwrap();
    let left = new_v7();
    let right = new_v7();
    let edge = new_v7();
    let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    writer
        .create_node(left, EntityTypeId::decode(0).unwrap())
        .unwrap();
    writer
        .create_node(right, EntityTypeId::decode(0).unwrap())
        .unwrap();
    writer.create_edge(edge, "KNOWS", &left, &right).unwrap();
    assert_eq!(writer.cancel_edges(&HashSet::from([to_bytes(&edge)])), 1);
    writer.refresh_topology_charge();
    assert_eq!(writer.buffered_topology_rows, 2);

    writer.create_edge(edge, "KNOWS", &left, &right).unwrap();
    fs::create_dir_all(dir.path().join("topology")).unwrap();
    fs::write(dir.path().join("topology/edges"), b"not a directory").unwrap();
    assert!(writer.flush().is_err());
    assert_eq!(writer.buffered_topology_rows, 1);
    assert!(writer.charged_topology_bytes <= writer.limits.max_buffered_topology_bytes);
    writer.release_committed_topology_state();
    assert!(writer.charged_topology_bytes > 0);
    assert_eq!(writer.cancel_edges(&HashSet::from([to_bytes(&edge)])), 1);
    assert_eq!(writer.charged_topology_bytes, 0);
}

#[test]
fn property_budget_preadmits_dynamic_values_and_releases_on_cancel() {
    let dir = TempDir::new().unwrap();
    let node = new_v7();
    let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS)
        .unwrap()
        .with_limits(GraphWriterLimits {
            max_buffered_topology_rows: 4,
            max_buffered_topology_bytes: 2_048,
            max_flush_scratch_bytes: 2_048,
        });
    writer
        .create_node(node, EntityTypeId::decode(0).unwrap())
        .unwrap();
    let before = writer.charged_topology_bytes;
    assert!(
        writer
            .set_properties(
                &node,
                Some("Person"),
                HashMap::from([("payload".into(), IrLiteral::Str("x".repeat(4_096)))]),
            )
            .is_err()
    );
    assert!(writer.properties.is_empty());
    assert_eq!(writer.charged_topology_bytes, before);

    writer
        .set_properties(
            &node,
            Some("Person"),
            HashMap::from([("name".into(), IrLiteral::Str("Ada".into()))]),
        )
        .unwrap();
    assert!(writer.charged_topology_bytes > before);
    assert!(writer.flush_scratch_bytes > NODE_SCRATCH_CHARGE);
    writer.cancel_nodes(&HashSet::from([to_bytes(&node)]));
    assert!(writer.properties.is_empty());
    assert_eq!(writer.charged_topology_bytes, 0);
    assert_eq!(writer.flush_scratch_bytes, 0);
}

#[test]
// Keep the three size points together so the bounded-doubling assertions
// describe one production write sequence.
#[allow(clippy::too_many_lines)]
fn cumulative_topology_and_index_work_doubles_with_bounded_windows() {
    const CHILD: &str = "GRAPHFORGE_931_SCALING_EVIDENCE_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("writer::tests::cumulative_topology_and_index_work_doubles_with_bounded_windows")
            .arg("--nocapture")
            .env(CHILD, "1")
            .status()
            .unwrap();
        assert!(status.success(), "isolated #931 scaling evidence failed");
        return;
    }
    let _measurement = crate::io_stats::test_measurement_guard();

    fn assert_linear_first_differences(label: &str, n: u64, twice: u64, four: u64) {
        let first = twice
            .checked_sub(n)
            .unwrap_or_else(|| panic!("{label}: 2N bytes regressed below N"));
        let second = four
            .checked_sub(twice)
            .unwrap_or_else(|| panic!("{label}: 4N bytes regressed below 2N"));
        assert!(first > 0, "{label}: N to 2N added no physical bytes");
        // The 4N-minus-2N increment represents twice as many new input
        // windows as the 2N-minus-N increment. Permit ten percent for
        // fixed Parquet and manifest metadata, but reject both
        // sub-linear omission and super-linear retained/output growth.
        let expected = first.saturating_mul(2);
        let tolerance = expected.div_ceil(10);
        assert!(
            second.abs_diff(expected) <= tolerance,
            "{label}: physical-byte first differences are not linear: N={n}, 2N={twice}, \
                 4N={four}, first={first}, second={second}, expected={expected} +/- {tolerance}"
        );
    }

    fn assert_linear_first_differences_with_fixed_overhead(
        label: &str,
        n: u64,
        twice: u64,
        four: u64,
        fixed_overhead: u64,
    ) {
        let first = twice
            .checked_sub(n)
            .unwrap_or_else(|| panic!("{label}: 2N bytes regressed below N"));
        let second = four
            .checked_sub(twice)
            .unwrap_or_else(|| panic!("{label}: 4N bytes regressed below 2N"));
        assert!(first > 0, "{label}: N to 2N added no physical bytes");
        let expected = first.saturating_mul(2);
        assert!(
            second.abs_diff(expected) <= fixed_overhead,
            "{label}: physical-work first differences are not linear within fixed format \
                 overhead: N={n}, 2N={twice}, 4N={four}, first={first}, second={second}, \
                 expected={expected} +/- {fixed_overhead}"
        );
    }

    fn retained_bytes(path: &Path) -> u64 {
        fs::read_dir(path)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let metadata = entry.metadata().unwrap();
                if metadata.is_dir() {
                    retained_bytes(&entry.path())
                } else {
                    metadata.len()
                }
            })
            .sum()
    }

    fn run(batches: u64) -> (u64, u64, u64, u64, u64, u64, TopologyWriteWork) {
        let dir = TempDir::new().unwrap();
        crate::io_stats::reset();
        let mut aggregate = TopologyWriteWork::default();
        for batch in 0..batches {
            let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS)
                .unwrap()
                .with_limits(GraphWriterLimits {
                    max_buffered_topology_rows: 4,
                    max_buffered_topology_bytes: 8 * 1024,
                    max_flush_scratch_bytes: 8 * 1024,
                });
            let left = Uuid::from_u128(10_000 + u128::from(batch) * 3);
            let right = Uuid::from_u128(10_001 + u128::from(batch) * 3);
            let edge = Uuid::from_u128(10_002 + u128::from(batch) * 3);
            writer
                .create_node(left, EntityTypeId::decode(0).unwrap())
                .unwrap();
            writer
                .create_node(right, EntityTypeId::decode(0).unwrap())
                .unwrap();
            writer
                .set_properties(
                    &left,
                    Some("Person"),
                    HashMap::from([("name".into(), IrLiteral::Str(format!("n{batch}")))]),
                )
                .unwrap();
            writer.create_edge(edge, "KNOWS", &left, &right).unwrap();
            writer.flush().unwrap();
            let work = writer.topology_write_work();
            aggregate.input_rows += work.input_rows;
            aggregate.prior_rows_decoded += work.prior_rows_decoded;
            aggregate.rows_encoded += work.rows_encoded;
            aggregate.shard_count += work.shard_count;
            aggregate.output_bytes += work.output_bytes;
            aggregate.new_rows_written += work.new_rows_written;
            aggregate.peak_buffered_rows =
                aggregate.peak_buffered_rows.max(work.peak_buffered_rows);
            aggregate.peak_buffered_bytes =
                aggregate.peak_buffered_bytes.max(work.peak_buffered_bytes);
            aggregate.peak_flush_scratch_bytes = aggregate
                .peak_flush_scratch_bytes
                .max(work.peak_flush_scratch_bytes);
            aggregate.uuid_input_records += work.uuid_input_records;
            aggregate.uuid_prior_topology_rows_decoded += work.uuid_prior_topology_rows_decoded;
            aggregate.uuid_physical_bytes_written += work.uuid_physical_bytes_written;
            aggregate.uuid_write_blocks += work.uuid_write_blocks;
            aggregate.uuid_write_bytes += work.uuid_write_bytes;
            aggregate.uuid_peak_buffered_records = aggregate
                .uuid_peak_buffered_records
                .max(work.uuid_peak_buffered_records);
            aggregate.uuid_peak_buffered_bytes = aggregate
                .uuid_peak_buffered_bytes
                .max(work.uuid_peak_buffered_bytes);
            aggregate.uuid_validation_blocks = aggregate
                .uuid_validation_blocks
                .saturating_add(work.uuid_validation_blocks);
            aggregate.uuid_validation_bytes = aggregate
                .uuid_validation_bytes
                .saturating_add(work.uuid_validation_bytes);
            aggregate.uuid_validation_random_seeks += work.uuid_validation_random_seeks;
        }
        let io = crate::io_stats::snapshot();
        (
            retained_bytes(dir.path()),
            aggregate.output_bytes,
            aggregate.uuid_physical_bytes_written,
            io.node_full_reads + io.node_filtered_reads,
            io.uuid_files_opened,
            io.uuid_files_synced,
            aggregate,
        )
    }

    let (n_bytes, n_topology_writes, n_uuid_writes, n_reads, n_opens, n_syncs, n) = run(8);
    let (
        twice_bytes,
        twice_topology_writes,
        twice_uuid_writes,
        twice_reads,
        twice_opens,
        twice_syncs,
        twice,
    ) = run(16);
    let (
        four_bytes,
        four_topology_writes,
        four_uuid_writes,
        four_reads,
        four_opens,
        four_syncs,
        four,
    ) = run(32);
    assert_linear_first_differences("retained footprint", n_bytes, twice_bytes, four_bytes);
    assert_linear_first_differences(
        "topology staged output",
        n_topology_writes,
        twice_topology_writes,
        four_topology_writes,
    );
    // This fixture's binary-carry merge levels add run-header, fence, and
    // manifest writes to an otherwise linear adjacent doubling interval.
    // Cap that disclosed amplification at a fixed 4 KiB for all size
    // points: a percentage or multiplicative bound would widen with input
    // and could conceal increasing super-linear index work.
    const UUID_INDEX_FIXED_OVERHEAD_BYTES: u64 = 4 * 1024;
    assert!(n_uuid_writes > 0);
    assert_linear_first_differences_with_fixed_overhead(
        "UUID index physical writes",
        n_uuid_writes,
        twice_uuid_writes,
        four_uuid_writes,
        UUID_INDEX_FIXED_OVERHEAD_BYTES,
    );
    assert_eq!((n_reads, twice_reads, four_reads), (0, 0, 0));
    assert!(n.uuid_validation_blocks > 0 && n.uuid_validation_bytes > 0);
    assert!(n_opens > 0 && n_syncs > 0);
    assert_linear_first_differences_with_fixed_overhead(
        "UUID validation blocks",
        n.uuid_validation_blocks,
        twice.uuid_validation_blocks,
        four.uuid_validation_blocks,
        0,
    );
    assert_linear_first_differences_with_fixed_overhead(
        "UUID validation bytes",
        n.uuid_validation_bytes,
        twice.uuid_validation_bytes,
        four.uuid_validation_bytes,
        4 * 1024,
    );
    assert_linear_first_differences_with_fixed_overhead(
        "UUID file opens",
        n_opens,
        twice_opens,
        four_opens,
        32,
    );
    assert_linear_first_differences_with_fixed_overhead(
        "UUID file syncs",
        n_syncs,
        twice_syncs,
        four_syncs,
        0,
    );
    assert_eq!(
        (
            n.prior_rows_decoded,
            twice.prior_rows_decoded,
            four.prior_rows_decoded,
            n.uuid_prior_topology_rows_decoded,
            twice.uuid_prior_topology_rows_decoded,
            four.uuid_prior_topology_rows_decoded,
            n.uuid_validation_random_seeks,
            twice.uuid_validation_random_seeks,
            four.uuid_validation_random_seeks,
        ),
        (0, 0, 0, 0, 0, 0, 0, 0, 0)
    );
    assert_eq!(twice.input_rows, n.input_rows * 2);
    assert_eq!(four.input_rows, n.input_rows * 4);
    assert_eq!(twice.rows_encoded, n.rows_encoded * 2);
    assert_eq!(four.rows_encoded, n.rows_encoded * 4);
    assert_eq!(twice.uuid_input_records, n.uuid_input_records * 2);
    assert_eq!(four.uuid_input_records, n.uuid_input_records * 4);
    assert_eq!(
        (
            n.peak_buffered_rows,
            twice.peak_buffered_rows,
            four.peak_buffered_rows
        ),
        (
            n.peak_buffered_rows,
            n.peak_buffered_rows,
            n.peak_buffered_rows
        )
    );
    assert!(four.peak_buffered_bytes <= 8 * 1024);
    assert!(four.peak_flush_scratch_bytes <= 8 * 1024);
}

#[test]
fn streaming_node_append_normalizes_legacy_scalar_labels() {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("topology")).unwrap();
    let legacy_schema = Arc::new(Schema::new(vec![
        uuid_field("node_uuid"),
        crate::schemas::id_field("node_id"),
        Field::new("type_id", DataType::UInt32, false),
        crate::schemas::ts_field("created_at"),
        crate::schemas::ts_field("updated_at"),
    ]));
    let uuid = FixedSizeBinaryArray::try_from_iter([vec![1_u8; 16]].into_iter()).unwrap();
    let ts =
        TimestampMicrosecondArray::from(vec![TS]).with_timezone_opt(Some(Arc::<str>::from("UTC")));
    let legacy = RecordBatch::try_new(
        legacy_schema,
        vec![
            Arc::new(uuid),
            Arc::new(UInt64Array::from(vec![1_u64])),
            Arc::new(UInt32Array::from(vec![7_u32])),
            Arc::new(ts.clone()),
            Arc::new(ts),
        ],
    )
    .unwrap();
    let file = File::create(dir.path().join("topology/nodes.parquet")).unwrap();
    let mut parquet = parquet::arrow::ArrowWriter::try_new(file, legacy.schema(), None).unwrap();
    parquet.write(&legacy).unwrap();
    parquet.close().unwrap();

    let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    assert_eq!(
        writer
            .create_node(new_v7(), EntityTypeId::decode(9).unwrap())
            .unwrap(),
        2
    );
    writer.flush().unwrap();

    let nodes = crate::catalog::read_nodes(dir.path()).unwrap();
    assert_eq!(nodes.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    let labels = nodes[0]
        .column_by_name("type_ids")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .unwrap();
    let first = labels.value(0);
    let first = first.as_any().downcast_ref::<UInt32Array>().unwrap();
    assert_eq!(first.values(), &[7]);
}

#[test]
fn create_edge_with_unknown_endpoint_errors() {
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    let a = new_v7();
    w.create_node(a, EntityTypeId::decode(0).unwrap()).unwrap();
    // `b` was never created.
    let b = new_v7();
    let e = w.create_edge(new_v7(), "KNOWS", &a, &b);
    assert!(matches!(e, Err(GfError::Storage(_))), "got {e:?}");

    let unknown_source = new_v7();
    let source_error = w.create_edge(new_v7(), "KNOWS", &unknown_source, &a);
    assert!(matches!(&source_error, Err(GfError::Storage(_))));
    assert!(source_error.unwrap_err().to_string().contains("source"));
}

#[test]
fn register_existing_node_resolves_edge_endpoint() {
    // #703: a MATCH-bound node is referenced (not created). Registering its
    // identity lets a subsequent edge resolve it without writing a node row
    // or advancing the surrogate counter.
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    // `a` already exists on disk with surrogate 42 (e.g. from a prior write).
    let a = new_v7();
    w.register_existing_node(a, 42).unwrap();
    // A freshly-minted `b` (next surrogate is 1 — register did not advance it).
    let b = new_v7();
    assert_eq!(
        w.create_node(b, EntityTypeId::decode(0).unwrap()).unwrap(),
        1
    );
    // The edge resolves both endpoints; its src_id is the registered 42.
    let edge_id = w
        .create_edge(new_v7(), "KNOWS", &a, &b)
        .expect("edge with a registered endpoint resolves");
    assert_eq!(edge_id, 1);
}

#[test]
fn register_existing_node_does_not_write_a_node_row() {
    // Registering an existing node must not buffer a NodeRow — only the one
    // genuinely-created node is flushed.
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    w.register_existing_node(new_v7(), 7).unwrap();
    let created = new_v7();
    w.create_node(created, EntityTypeId::decode(0).unwrap())
        .unwrap();
    w.flush().unwrap();
    // Exactly one node on disk (the created one), not two.
    let nodes = crate::catalog::read_nodes(dir.path()).unwrap();
    let rows: usize = nodes.iter().map(arrow::array::RecordBatch::num_rows).sum();
    assert_eq!(
        rows, 1,
        "register_existing_node must not persist a node row"
    );
}

#[test]
fn empty_flush_creates_no_directories() {
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    // Owned layout admission creates only its protocol metadata. Empty flush
    // must add no data files or rewrite that admitted metadata.
    let before = crate::capture_graph_files(dir.path()).unwrap().0;
    w.flush().unwrap();
    assert_eq!(crate::capture_graph_files(dir.path()).unwrap().0, before);
    assert!(!dir.path().join("properties").exists());
}

/// Read a node-property file back into a `uuid → props` map (mirrors the
/// decode the rewrite primitives use), for assertions.
pub(super) fn read_node_props(
    dir: &Path,
    stem: &str,
) -> HashMap<[u8; 16], HashMap<String, IrLiteral>> {
    read_node_property_rows(dir, stem).unwrap()
}

pub(super) fn read_edge_props(
    dir: &Path,
    stem: &str,
) -> HashMap<[u8; 16], HashMap<String, IrLiteral>> {
    let batches = crate::catalog::read_edge_properties(dir, stem).unwrap();
    let mut out = HashMap::new();
    for row in decode_edge_property_rows(&batches).unwrap() {
        out.insert(row.edge_uuid, row.props);
    }
    out
}

#[test]
fn pending_nodes_batch_is_canonical_and_non_consuming() {
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    let empty = w.pending_nodes_batch().unwrap();
    assert_eq!(empty.schema(), TOPOLOGY_NODES_SCHEMA.clone());
    assert_eq!(empty.num_rows(), 0);

    let node = new_v7();
    w.create_node_with_labels(
        node,
        &[
            EntityTypeId::decode(3).unwrap(),
            EntityTypeId::decode(7).unwrap(),
        ],
    )
    .unwrap();
    for _ in 0..2 {
        let batch = w.pending_nodes_batch().unwrap();
        assert_eq!(batch.schema(), TOPOLOGY_NODES_SCHEMA.clone());
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column_by_name("node_id")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            1
        );
    }
    assert!(w.contains_pending_node(&to_bytes(&node)));
}

#[test]
fn cancel_nodes_drops_rows_props_and_mapping() {
    let dir = TempDir::new().unwrap();
    let (a, b) = (new_v7(), new_v7());
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    w.create_node(a, EntityTypeId::decode(0).unwrap()).unwrap();
    w.create_node(b, EntityTypeId::decode(0).unwrap()).unwrap();
    w.set_properties(
        &a,
        None,
        HashMap::from([("name".to_owned(), IrLiteral::Str("A".into()))]),
    )
    .unwrap();

    assert!(w.contains_pending_node(&to_bytes(&a)));
    let dropped = w.cancel_nodes(&HashSet::from([to_bytes(&a)]));
    assert_eq!(dropped, 1);
    assert!(!w.contains_pending_node(&to_bytes(&a)));

    // The mapping is forgotten: an edge to the cancelled node must fail.
    let err = w.create_edge(new_v7(), "KNOWS", &b, &a);
    assert!(err.is_err(), "edge to a cancelled node must fail");

    w.flush().unwrap();
    // Only b hit disk; a's property row never did.
    let nodes = crate::catalog::read_nodes(dir.path()).unwrap();
    let total: usize = nodes.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 1, "only b persisted");
    assert!(
        !read_node_props(dir.path(), "_untyped").contains_key(&to_bytes(&a)),
        "cancelled node's props never hit disk"
    );
}

#[test]
fn cancel_edges_drops_rows_and_edge_props() {
    let dir = TempDir::new().unwrap();
    let (a, b) = (new_v7(), new_v7());
    let (e1, e2) = (new_v7(), new_v7());
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    w.create_node(a, EntityTypeId::decode(0).unwrap()).unwrap();
    w.create_node(b, EntityTypeId::decode(0).unwrap()).unwrap();
    w.create_edge(e1, "KNOWS", &a, &b).unwrap();
    w.create_edge(e2, "KNOWS", &b, &a).unwrap();
    w.set_edge_properties(
        &e1,
        Some("KNOWS"),
        HashMap::from([("since".to_owned(), IrLiteral::Int(2020))]),
    )
    .unwrap();

    assert!(w.contains_pending_edge(&to_bytes(&e1)));
    assert_eq!(w.cancel_edges(&HashSet::from([to_bytes(&e1)])), 1);
    assert!(!w.contains_pending_edge(&to_bytes(&e1)));

    w.flush().unwrap();
    assert!(
        !read_edge_props(dir.path(), "KNOWS").contains_key(&to_bytes(&e1)),
        "cancelled edge's props never hit disk"
    );
    let edges = crate::catalog::read_edges_from_inventory(
        &test_inventory(dir.path()),
        "*",
        OntologyMode::Exploratory,
    )
    .unwrap();
    let total: usize = edges.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 1, "only e2 persisted");
}

#[test]
fn pending_incident_edge_uuids_sees_buffered_edges() {
    let dir = TempDir::new().unwrap();
    let (a, b, c) = (new_v7(), new_v7(), new_v7());
    let e_ab = new_v7();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    w.create_node(a, EntityTypeId::decode(0).unwrap()).unwrap();
    w.create_node(b, EntityTypeId::decode(0).unwrap()).unwrap();
    w.create_node(c, EntityTypeId::decode(0).unwrap()).unwrap();
    w.create_edge(e_ab, "KNOWS", &a, &b).unwrap();

    // Incident from either endpoint; c has none.
    let hits = w.pending_incident_edge_uuids(&HashSet::from([to_bytes(&b)]));
    assert_eq!(hits, vec![to_bytes(&e_ab)]);
    assert!(
        w.pending_incident_edge_uuids(&HashSet::from([to_bytes(&c)]))
            .is_empty()
    );
}

#[test]
fn pending_query_and_label_edits_are_exact_before_flush_and_reopen() {
    let dir = TempDir::new().unwrap();
    let (alice, bob, edge) = (new_v7(), new_v7(), new_v7());
    let (alice_bytes, bob_bytes, edge_bytes) = (to_bytes(&alice), to_bytes(&bob), to_bytes(&edge));
    let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    writer
        .create_node_with_labels(
            alice,
            &[
                EntityTypeId::decode(3).unwrap(),
                EntityTypeId::decode(7).unwrap(),
            ],
        )
        .unwrap();
    writer
        .create_node(bob, EntityTypeId::decode(3).unwrap())
        .unwrap();
    writer
        .set_properties(
            &alice,
            Some("Person"),
            HashMap::from([
                ("name".into(), IrLiteral::Str("Alice".into())),
                ("age".into(), IrLiteral::Int(42)),
            ]),
        )
        .unwrap();

    assert_eq!(
        writer.pending_node_labels(&HashSet::from([alice_bytes, bob_bytes])),
        HashSet::from([
            EntityTypeId::decode(3).unwrap(),
            EntityTypeId::decode(7).unwrap()
        ])
    );
    let matched = writer
        .find_pending_node(
            &[
                EntityTypeId::decode(3).unwrap(),
                EntityTypeId::decode(7).unwrap(),
            ],
            &[("name".into(), IrLiteral::Str("Alice".into()))],
        )
        .unwrap();
    assert_eq!(matched.0, alice_bytes);
    assert_eq!(matched.2.encode(), 3);
    assert_eq!(
        matched.3.iter().map(|id| id.encode()).collect::<Vec<_>>(),
        vec![3, 7]
    );
    assert_eq!(matched.4["age"], IrLiteral::Int(42));
    assert!(
        writer
            .find_pending_node(&[EntityTypeId::decode(9).unwrap()], &[])
            .is_none()
    );
    assert!(
        writer
            .find_pending_node(
                &[EntityTypeId::decode(3).unwrap()],
                &[("name".into(), IrLiteral::Str("Bob".into()))]
            )
            .is_none()
    );

    assert_eq!(
        writer.add_pending_node_labels(
            &alice_bytes,
            &[
                EntityTypeId::decode(7).unwrap(),
                EntityTypeId::decode(9).unwrap()
            ]
        ),
        1
    );
    assert_eq!(
        writer.add_pending_node_labels(&[0xff; 16], &[EntityTypeId::decode(1).unwrap()]),
        0
    );
    assert_eq!(
        writer.remove_pending_node_labels(
            &alice_bytes,
            &[
                EntityTypeId::decode(7).unwrap(),
                EntityTypeId::decode(99).unwrap()
            ]
        ),
        1
    );
    assert_eq!(
        writer.remove_pending_node_labels(&[0xff; 16], &[EntityTypeId::decode(1).unwrap()]),
        0
    );
    assert_eq!(
        writer.pending_node_labels(&HashSet::from([alice_bytes])),
        HashSet::from([
            EntityTypeId::decode(3).unwrap(),
            EntityTypeId::decode(9).unwrap()
        ])
    );

    writer.create_edge(edge, "KNOWS", &alice, &bob).unwrap();
    writer
        .set_edge_properties(
            &edge,
            Some("KNOWS"),
            HashMap::from([("since".into(), IrLiteral::Int(2024))]),
        )
        .unwrap();
    let direct = writer
        .find_pending_edge(
            "KNOWS",
            &alice_bytes,
            &bob_bytes,
            false,
            &[("since".into(), IrLiteral::Int(2024))],
        )
        .unwrap();
    assert_eq!(direct.0, edge_bytes);
    assert_eq!(direct.1, alice_bytes);
    assert_eq!(direct.2, bob_bytes);
    assert_eq!(direct.3["since"], IrLiteral::Int(2024));
    assert!(
        writer
            .find_pending_edge("KNOWS", &bob_bytes, &alice_bytes, false, &[])
            .is_none()
    );
    assert!(
        writer
            .find_pending_edge("KNOWS", &bob_bytes, &alice_bytes, true, &[])
            .is_some()
    );
    assert!(
        writer
            .find_pending_edge("IGNORES", &alice_bytes, &bob_bytes, false, &[])
            .is_none()
    );

    writer.flush().unwrap();
    let mut reopened = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS + 1).unwrap();
    assert_eq!(
        reopened
            .create_node(new_v7(), EntityTypeId::decode(3).unwrap())
            .unwrap(),
        3
    );
    assert_eq!(
        read_node_props(dir.path(), "Person")[&alice_bytes]["name"],
        IrLiteral::Str("Alice".into())
    );
    assert_eq!(
        read_edge_props(dir.path(), "KNOWS")[&edge_bytes]["since"],
        IrLiteral::Int(2024)
    );
}

#[test]
fn merge_and_remove_pending_props_edit_buffered_rows() {
    let dir = TempDir::new().unwrap();
    let a = new_v7();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    w.create_node(a, EntityTypeId::decode(0).unwrap()).unwrap();
    w.set_properties(
        &a,
        None,
        HashMap::from([
            ("name".to_owned(), IrLiteral::Str("old".into())),
            ("age".to_owned(), IrLiteral::Int(30)),
        ]),
    )
    .unwrap();

    // SET on the pending node: overwrite one key, add another.
    w.merge_pending_node_props(
        &to_bytes(&a),
        None,
        HashMap::from([
            ("name".to_owned(), IrLiteral::Str("new".into())),
            ("city".to_owned(), IrLiteral::Str("Oslo".into())),
        ]),
    )
    .unwrap();
    // REMOVE on the pending node: drop a key; absent keys are no-ops.
    w.remove_pending_node_props(
        &to_bytes(&a),
        &HashSet::from(["age".to_owned(), "absent".to_owned()]),
    );

    w.flush().unwrap();
    let props = &read_node_props(dir.path(), "_untyped")[&to_bytes(&a)];
    assert_eq!(props["name"], IrLiteral::Str("new".into()));
    assert_eq!(props["city"], IrLiteral::Str("Oslo".into()));
    assert!(!props.contains_key("age"), "removed before flush");
}

#[test]
fn flush_into_composes_with_staged_delete_in_one_batch() {
    // The #792 statement shape: DELETE a committed node and CREATE a new
    // one in the same statement — one RewriteBatch, one commit, with the
    // flush reading through the delete's staged nodes.parquet content.
    let dir = TempDir::new().unwrap();
    let (a, b) = (new_v7(), new_v7());
    let mut seed = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    seed.create_node(a, EntityTypeId::decode(0).unwrap())
        .unwrap();
    seed.create_node(b, EntityTypeId::decode(0).unwrap())
        .unwrap();
    seed.set_properties(
        &a,
        None,
        HashMap::from([("name".to_owned(), IrLiteral::Str("A".into()))]),
    )
    .unwrap();
    seed.flush().unwrap();

    let mut staged = RewriteBatch::new();
    let removed =
        crate::mutator::stage_delete_nodes(&mut staged, dir.path(), &HashSet::from([to_bytes(&a)]))
            .unwrap();
    assert_eq!(removed, 1);

    let d = new_v7();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    w.create_node(d, EntityTypeId::decode(0).unwrap()).unwrap();
    w.flush_into(&mut staged).unwrap();

    // nodes.parquet staged exactly once (net content), still last-ish in
    // commit order relative to the delete's property rewrite.
    let staged_nodes = staged
        .staged_paths()
        .filter(|p| p.ends_with("topology/nodes.parquet"))
        .count();
    assert_eq!(staged_nodes, 1, "net content, no double-stage");

    // Nothing visible before commit.
    let pre: usize = crate::catalog::read_nodes(dir.path())
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(pre, 2);

    w.commit_topology_aware_with_uuid_index(staged, vec![a], Vec::new())
        .unwrap();
    let nodes = crate::catalog::read_nodes(dir.path()).unwrap();
    let total: usize = nodes.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 2, "b survives, a deleted, d created");
    assert!(
        !read_node_props(dir.path(), "_untyped").contains_key(&to_bytes(&a)),
        "deleted node's props gone"
    );
}

#[test]
fn committed_snapshot_refresh_failure_never_restages_rows() {
    let dir = TempDir::new().unwrap();
    let node = new_v7();
    let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    writer
        .create_node(node, EntityTypeId::decode(0).unwrap())
        .unwrap();
    crate::uuid_membership::fail_next_snapshot_refresh_for_test();

    let error = writer.flush().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("committed but UUID index snapshot refresh failed")
    );
    assert!(writer.pending_index_nodes.is_empty());
    assert!(writer.nodes.is_empty());
    let committed_generation = crate::read_topology_generation(dir.path()).unwrap();
    assert_eq!(committed_generation, 1);
    assert!(
        dir.path()
            .join("topology/uuid-membership/topology-receipt.json")
            .is_file()
    );

    writer.flush().unwrap();
    assert_eq!(
        crate::read_topology_generation(dir.path()).unwrap(),
        committed_generation
    );
    let batches = crate::catalog::read_nodes(dir.path()).unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let mut index = crate::UuidMembershipIndex::open(dir.path()).unwrap();
    assert_eq!(index.count(crate::UuidIndexKind::Node), 1);
    assert_eq!(
        index.probe(crate::UuidIndexKind::Node, &[node]).unwrap().0,
        [true]
    );
}
