use super::GraphCatalog;
use super::GraphSchema;
use super::admitted_parquet;
use super::count_edge_rows_from_inventory;
use super::filtered_parquet::project_batches_with_key;
use super::io_err;
use super::max_edge_id;
use super::max_node_id;
use super::normalize_topology_nodes;
use super::parquet_err;
use super::read_edge_properties_from_inventory;
use super::read_edges;
use super::read_edges_filtered;
use super::read_edges_filtered_from_inventory;
use super::read_edges_filtered_observed_from_inventory;
use super::read_edges_filtered_projected_from_inventory;
use super::read_edges_from_inventory;
use super::read_nodes;
use super::read_nodes_filtered;
use super::read_nodes_filtered_projected_observed;
use super::read_parquet_or_empty;
use super::read_properties;
use super::total_rows;
use super::visit_nodes_batched;
use crate::schemas::EXPLORATORY_EDGE_SCHEMA;
use crate::schemas::TOPOLOGY_NODES_SCHEMA;
use crate::schemas::TYPED_EDGE_SCHEMA;
use arrow::array::Array;
use arrow::array::FixedSizeBinaryArray;
use arrow::array::RecordBatch;
use arrow::array::StringArray;
use arrow::array::TimestampMicrosecondArray;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use datafusion::catalog::CatalogProvider;
use datafusion::catalog::SchemaProvider;
use graphforge_core::OntologyMode;
use graphforge_ir::RuntimeCatalog;
use graphforge_value::EntityTypeId;
use graphforge_value::RelationTypeId;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn admitted_topology_requires_payload_except_empty_selection() {
    let dir = TempDir::new().unwrap();
    let node = graphforge_core::uuid::new_v7();
    let mut writer = crate::GraphWriter::open_at(dir.path(), OntologyMode::Strict, 1).unwrap();
    writer
        .create_node(node, EntityTypeId::decode(0).unwrap())
        .unwrap();
    writer
        .create_edge(graphforge_core::uuid::new_v7(), "CON", &node, &node)
        .unwrap();
    writer.flush().unwrap();
    drop(writer);
    let catalog = GraphCatalog::open(dir.path(), None, &RuntimeCatalog::new()).unwrap();
    let inventory = catalog.admitted_inventory().unwrap();
    let path = inventory.edge_files(Some("CON"))[0].1.clone();
    std::fs::remove_file(&path).unwrap();
    let ids = std::collections::HashSet::from([0]);
    let empty = std::collections::HashSet::new();
    for route in ["CON", "*"] {
        assert!(read_edges_from_inventory(&inventory, route, OntologyMode::Strict).is_err());
        assert!(count_edge_rows_from_inventory(&inventory, route, OntologyMode::Strict).is_err());
        assert!(
            read_edges_filtered_from_inventory(&inventory, route, OntologyMode::Strict, &ids)
                .is_err()
        );
        assert!(
            read_edges_filtered_projected_from_inventory(
                &inventory,
                route,
                OntologyMode::Strict,
                &ids,
                &[0],
                None
            )
            .is_err()
        );
        assert_eq!(
            total_rows(
                &read_edges_filtered_from_inventory(
                    &inventory,
                    route,
                    OntologyMode::Strict,
                    &empty
                )
                .unwrap()
            ),
            0
        );
        let observer = Arc::new(Wave12Observer::default());
        let observed: Arc<dyn crate::io_stats::FilteredReadObserver> = observer.clone();
        let projected = read_edges_filtered_projected_from_inventory(
            &inventory,
            route,
            OntologyMode::Strict,
            &empty,
            &[0],
            Some(&observed),
        )
        .unwrap();
        let schema = if route == "*" {
            &EXPLORATORY_EDGE_SCHEMA
        } else {
            &TYPED_EDGE_SCHEMA
        };
        let expected = project_batches_with_key(Vec::new(), schema, &[0], "edge_id").unwrap();
        assert_eq!(projected[0].schema(), expected[0].schema());
        assert_eq!(total_rows(&projected), 0);
        let filtered = read_edges_filtered_observed_from_inventory(
            &inventory,
            route,
            OntologyMode::Strict,
            &empty,
            Some(&observed),
        )
        .unwrap();
        assert_eq!(total_rows(&filtered), 0);
        for counter in [
            &observer.started,
            &observer.scanned,
            &observer.completed,
            &observer.failed,
            &observer.pruning,
        ] {
            assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 0);
        }
    }
    assert_eq!(
        total_rows(&read_parquet_or_empty(&path, TYPED_EDGE_SCHEMA.clone()).unwrap()),
        0
    );
}

#[test]
fn catalog_parquet_admission_rejects_directory_and_bad_leading_magic() {
    let root = TempDir::new().unwrap();
    assert!(admitted_parquet(root.path()).is_err());
    let bad = root.path().join("bad.parquet");
    std::fs::write(&bad, b"NOPE\0\0\0\0PAR1").unwrap();
    assert!(
        admitted_parquet(&bad)
            .unwrap_err()
            .to_string()
            .contains("leading magic")
    );
    let bad_footer = root.path().join("bad-footer.parquet");
    std::fs::write(&bad_footer, b"PAR1\0\0\0\0NOPE").unwrap();
    assert!(admitted_parquet(&bad_footer).is_err());
}

#[cfg(unix)]
#[test]
fn catalog_parquet_admission_rejects_symlink_and_fifo_without_blocking() {
    use std::os::unix::fs::symlink;

    let root = TempDir::new().unwrap();
    let target = root.path().join("target.parquet");
    std::fs::write(&target, b"NOPE\0\0\0\0PAR1").unwrap();
    let link = root.path().join("link.parquet");
    symlink(&target, &link).unwrap();
    assert!(admitted_parquet(&link).is_err());

    let fifo = root.path().join("pipe.parquet");
    assert!(
        std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success()
    );
    assert!(admitted_parquet(&fifo).is_err());
}

#[tokio::test]
async fn mapped_writer_reopen_and_catalog_refresh_preserve_semantic_routes() {
    let dir = TempDir::new().unwrap();
    let left = graphforge_core::uuid::new_v7();
    let right = graphforge_core::uuid::new_v7();
    let first_edge = graphforge_core::uuid::new_v7();
    let mut writer = crate::GraphWriter::open_at(dir.path(), OntologyMode::Strict, 1).unwrap();
    for node in [left, right] {
        writer
            .create_node(node, EntityTypeId::decode(0).unwrap())
            .unwrap();
    }
    writer
        .create_edge(first_edge, "CON", &left, &right)
        .unwrap();
    writer
        .set_properties(
            &left,
            Some("con"),
            HashMap::from([("rank".into(), graphforge_ir::IrLiteral::Int(7))]),
        )
        .unwrap();
    writer
        .set_edge_properties(
            &first_edge,
            Some("CON"),
            HashMap::from([("weight".into(), graphforge_ir::IrLiteral::Int(11))]),
        )
        .unwrap();
    writer.flush().unwrap();
    drop(writer);
    let mut catalog = GraphCatalog::open(dir.path(), None, &RuntimeCatalog::new()).unwrap();
    catalog.refresh_property_inventory(dir.path()).unwrap();
    let semantic_id = RelationTypeId::decode(0).unwrap();
    catalog
        .semantic_rel_routes
        .insert(semantic_id, "CON".into());
    let old = catalog.semantic_edge_table(semantic_id).unwrap();
    let inventory = catalog.admitted_inventory().unwrap();
    assert_eq!(
        inventory
            .routes(crate::PropertyRouteKind::Node)
            .collect::<Vec<_>>(),
        ["con"]
    );
    assert_eq!(
        inventory
            .routes(crate::PropertyRouteKind::Edge)
            .collect::<Vec<_>>(),
        ["CON"]
    );
    let node = read_properties(dir.path(), "con").unwrap();
    let rank = node[0]
        .column_by_name("rank")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(rank.null_count(), 0);
    assert_eq!(rank.values().as_ref(), &[7]);
    let edge = read_edge_properties_from_inventory(dir.path(), &inventory, "CON").unwrap();
    let weight = edge[0]
        .column_by_name("weight")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(weight.null_count(), 0);
    assert_eq!(weight.values().as_ref(), &[11]);
    let mut writer = crate::GraphWriter::open_at(dir.path(), OntologyMode::Strict, 2).unwrap();
    writer.register_existing_node(left, 1).unwrap();
    writer.register_existing_node(right, 2).unwrap();
    writer
        .create_edge(graphforge_core::uuid::new_v7(), "CON", &right, &left)
        .unwrap();
    writer.flush().unwrap();
    drop(writer);
    catalog.refresh_property_inventory(dir.path()).unwrap();
    let ctx = datafusion::prelude::SessionContext::new();
    let direct = catalog
        .schema("graph")
        .unwrap()
        .table("edges_CON")
        .await
        .unwrap()
        .unwrap();
    let semantic = catalog.semantic_edge_table(semantic_id).unwrap();
    for (provider, expected) in [(old, 1), (direct, 2), (semantic, 2)] {
        let plan = provider.scan(&ctx.state(), None, &[], None).await.unwrap();
        let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();
        assert_eq!(total_rows(&batches), expected);
        for batch in batches {
            let ids = batch
                .column_by_name("edge_uuid")
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            assert_eq!(ids.null_count(), 0);
        }
    }
}

#[test]
fn raw_catalog_shares_one_authenticated_property_inventory() {
    let dir = TempDir::new().unwrap();
    let catalog = GraphCatalog::open(dir.path(), None, &RuntimeCatalog::new()).unwrap();
    let catalog_authority = catalog
        .schema
        .authority
        .read()
        .expect("graph catalog authority lock");
    let authority = catalog_authority
        .property_inventory
        .as_ref()
        .expect("raw catalog admits one complete property authority");
    let node = catalog.property_table(dir.path(), "Person");
    let edge = catalog.edge_property_table(dir.path(), "KNOWS");
    assert!(Arc::ptr_eq(
        authority,
        node.inventory.as_ref().expect("node authority")
    ));
    assert!(Arc::ptr_eq(
        authority,
        edge.inventory.as_ref().expect("edge authority")
    ));
}

#[test]
fn parquet_and_io_error_helpers_preserve_external_messages() {
    let parquet = parquet_err("parquet boom");
    assert!(parquet.to_string().contains("parquet boom"));
    let io = io_err(&std::io::Error::other("io boom"));
    assert!(io.to_string().contains("io boom"));
}

#[derive(Default)]
pub(super) struct Wave12Observer {
    pub(super) started: std::sync::atomic::AtomicUsize,
    pub(super) scanned: std::sync::atomic::AtomicUsize,
    pub(super) completed: std::sync::atomic::AtomicUsize,
    pub(super) failed: std::sync::atomic::AtomicUsize,
    pub(super) pruning: std::sync::atomic::AtomicUsize,
}

impl crate::io_stats::FilteredReadObserver for Wave12Observer {
    fn read_started(&self, _: crate::io_stats::FilteredReadTable) {
        self.started
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn rows_scanned(&self, _: crate::io_stats::FilteredReadTable, rows: u64) {
        self.scanned
            .fetch_add(rows as usize, std::sync::atomic::Ordering::Relaxed);
    }

    fn read_completed(&self, _: crate::io_stats::FilteredReadTable, rows: u64, _: bool) {
        self.completed
            .fetch_add(rows as usize, std::sync::atomic::Ordering::Relaxed);
    }

    fn read_failed(&self, _: crate::io_stats::FilteredReadTable) {
        self.failed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn pruning(
        &self,
        _: crate::io_stats::FilteredReadTable,
        _: crate::io_stats::FilteredReadPruning,
    ) {
        self.pruning
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

pub(super) fn write_nodes_parquet(path: &Path) {
    write_nodes_parquet_value(path, 1, 1);
}

pub(super) fn write_nodes_parquet_value(path: &Path, uuid_byte: u8, node_id: u64) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let uuid_bytes: Vec<u8> = vec![uuid_byte; 16];
    let uuid_arr =
        FixedSizeBinaryArray::try_from_iter(std::iter::once(uuid_bytes.clone())).unwrap();
    let ts = TimestampMicrosecondArray::from(vec![0i64]).with_timezone_opt(Some(Arc::from("UTC")));
    let labels = arrow::array::ListArray::new(
        Arc::new(Field::new("item", DataType::UInt32, false)),
        OffsetBuffer::new(vec![0, 1].into()),
        Arc::new(UInt32Array::from(vec![0u32])),
        None,
    );

    let batch = RecordBatch::try_new(
        TOPOLOGY_NODES_SCHEMA.clone(),
        vec![
            Arc::new(uuid_arr),
            Arc::new(UInt64Array::from(vec![node_id])),
            Arc::new(UInt32Array::from(vec![0u32])),
            Arc::new(labels),
            Arc::new(ts.clone()),
            Arc::new(ts),
        ],
    )
    .unwrap();

    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(
        file,
        TOPOLOGY_NODES_SCHEMA.clone(),
        Some(WriterProperties::builder().build()),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[test]
fn ordinary_node_readers_union_legacy_and_canonical_shards() {
    let dir = TempDir::new().unwrap();
    write_nodes_parquet_value(&dir.path().join("topology/nodes.parquet"), 1, 1);
    write_nodes_parquet_value(
        &dir.path()
            .join("topology/nodes/00000000000000000002-00000000000000000002.parquet"),
        2,
        2,
    );
    let batches = read_nodes(dir.path()).unwrap();
    assert_eq!(total_rows(&batches), 2);
    assert_eq!(max_node_id(dir.path()).unwrap(), 2);
    let filtered = read_nodes_filtered(dir.path(), &std::collections::HashSet::from([2])).unwrap();
    assert_eq!(total_rows(&filtered), 1);
    let mut visited = 0;
    visit_nodes_batched(dir.path(), 1, |batch| {
        visited += batch.num_rows();
        Ok(true)
    })
    .unwrap();
    assert_eq!(visited, 2);

    std::fs::remove_file(dir.path().join("topology/nodes.parquet")).unwrap();
    let shard_only = read_nodes(dir.path()).unwrap();
    assert_eq!(total_rows(&shard_only), 1);
    assert_eq!(max_node_id(dir.path()).unwrap(), 2);
}

#[test]
fn legacy_scalar_node_labels_normalize_to_singleton_sets() {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("topology")).unwrap();
    let old_schema = Arc::new(Schema::new(vec![
        crate::schemas::uuid_field("node_uuid"),
        crate::schemas::id_field("node_id"),
        Field::new("type_id", DataType::UInt32, false),
        crate::schemas::ts_field("created_at"),
        crate::schemas::ts_field("updated_at"),
    ]));
    let uuid = FixedSizeBinaryArray::try_from_iter([vec![1u8; 16]].into_iter()).unwrap();
    let ts = TimestampMicrosecondArray::from(vec![0i64]).with_timezone_opt(Some(Arc::from("UTC")));
    let legacy = RecordBatch::try_new(
        old_schema,
        vec![
            Arc::new(uuid),
            Arc::new(UInt64Array::from(vec![1])),
            Arc::new(UInt32Array::from(vec![7])),
            Arc::new(ts.clone()),
            Arc::new(ts),
        ],
    )
    .unwrap();

    let file = File::create(dir.path().join("topology/nodes.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, legacy.schema(), None).unwrap();
    writer.write(&legacy).unwrap();
    writer.close().unwrap();

    let normalized = read_nodes(dir.path()).unwrap();
    assert_eq!(normalized[0].schema(), TOPOLOGY_NODES_SCHEMA.clone());
    let labels = normalized[0]
        .column_by_name("type_ids")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .unwrap();
    let values = labels.value(0);
    let values = values.as_any().downcast_ref::<UInt32Array>().unwrap();
    assert_eq!(values.values(), &[7]);

    let projected = read_nodes_filtered_projected_observed(
        dir.path(),
        &std::collections::HashSet::from([1]),
        &[TOPOLOGY_NODES_SCHEMA.index_of("type_ids").unwrap()],
        None,
    )
    .unwrap();
    assert_eq!(projected[0].num_columns(), 2);
    assert!(projected[0].column_by_name("type_ids").is_some());
    assert!(projected[0].column_by_name("node_id").is_some());
}

pub(super) fn write_edge_parquet(path: &Path) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let fsb = |v: Vec<u8>| FixedSizeBinaryArray::try_from_iter(std::iter::once(v)).unwrap();
    let ts = TimestampMicrosecondArray::from(vec![0i64]).with_timezone_opt(Some(Arc::from("UTC")));

    let batch = RecordBatch::try_new(
        TYPED_EDGE_SCHEMA.clone(),
        vec![
            Arc::new(fsb(vec![2u8; 16])),
            Arc::new(fsb(vec![1u8; 16])),
            Arc::new(fsb(vec![3u8; 16])),
            Arc::new(UInt64Array::from(vec![1u64])),
            Arc::new(UInt64Array::from(vec![1u64])),
            Arc::new(UInt64Array::from(vec![2u64])),
            Arc::new(ts),
        ],
    )
    .unwrap();

    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(
        file,
        TYPED_EDGE_SCHEMA.clone(),
        Some(WriterProperties::builder().build()),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[test]
fn graph_catalog_open_exploratory_registers_tables() {
    let dir = TempDir::new().unwrap();
    let catalog = RuntimeCatalog::new();
    let gc = GraphCatalog::open(dir.path(), None, &catalog).unwrap();
    let schema = gc.schema("graph").unwrap();
    let names = schema.table_names();
    assert!(
        names.contains(&"topology_nodes".to_owned()),
        "got {names:?}"
    );
    assert!(
        names.contains(&"edges__exploratory".to_owned()),
        "got {names:?}"
    );
}

#[test]
fn graph_catalog_schema_names() {
    let dir = TempDir::new().unwrap();
    let catalog = RuntimeCatalog::new();
    let gc = GraphCatalog::open(dir.path(), None, &catalog).unwrap();
    assert_eq!(gc.schema_names(), vec!["graph"]);
}

// -----------------------------------------------------------------------
// Direct readers (read_edges / read_nodes) — #580
// -----------------------------------------------------------------------

pub(super) fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

#[test]
fn read_edges_strict_returns_typed_rows() {
    let dir = TempDir::new().unwrap();
    write_edge_parquet(
        &dir.path()
            .join("topology")
            .join("edges")
            .join("KNOWS.parquet"),
    );

    let batches = read_edges(dir.path(), "KNOWS", OntologyMode::Strict).unwrap();
    assert_eq!(row_count(&batches), 1);
    // Strict mode reads the typed schema (no rel_type_name column).
    assert_eq!(batches[0].schema(), TYPED_EDGE_SCHEMA.clone());
    assert!(
        batches[0]
            .schema()
            .field_with_name("rel_type_name")
            .is_err()
    );
}

#[test]
fn read_edges_rejects_path_traversal_rel_name() {
    let dir = TempDir::new().unwrap();
    for bad in ["../secret", "a/b", "..", "/etc/passwd"] {
        let err = read_edges(dir.path(), bad, OntologyMode::Strict).unwrap_err();
        assert!(
            err.to_string().contains("invalid relation name"),
            "expected rejection for {bad:?}, got: {err}"
        );
    }
    // Exploratory mode uses a fixed stem, so a traversal-looking rel_name is
    // harmless (never reaches the path) — it must NOT error.
    assert!(read_edges(dir.path(), "../secret", OntologyMode::Exploratory).is_ok());
}

#[test]
fn read_edges_missing_file_returns_empty_typed_batch() {
    let dir = TempDir::new().unwrap();
    // No edge file written.
    let batches = read_edges(dir.path(), "KNOWS", OntologyMode::Strict).unwrap();
    assert_eq!(row_count(&batches), 0);
    assert_eq!(batches[0].schema(), TYPED_EDGE_SCHEMA.clone());
}

#[test]
fn read_edges_exploratory_uses_exploratory_file_and_schema() {
    let dir = TempDir::new().unwrap();
    // Exploratory edges live in `_exploratory.parquet`; a typed `KNOWS.parquet`
    // must be ignored in this mode.
    write_edge_parquet(
        &dir.path()
            .join("topology")
            .join("edges")
            .join("KNOWS.parquet"),
    );

    let batches = read_edges(dir.path(), "KNOWS", OntologyMode::Exploratory).unwrap();
    // The exploratory file does not exist → empty batch with the exploratory
    // schema (which carries rel_type_name), NOT the typed KNOWS rows.
    assert_eq!(row_count(&batches), 0);
    assert_eq!(batches[0].schema(), EXPLORATORY_EDGE_SCHEMA.clone());
    assert!(batches[0].schema().field_with_name("rel_type_name").is_ok());
}

// -----------------------------------------------------------------------
// Untyped wildcard union read (#823)
// -----------------------------------------------------------------------

/// Write a one-row typed edge file with the given surrogates.
pub(super) fn write_typed_edge(path: &Path, edge_id: u64, src_id: u64, dst_id: u64) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let fsb = |v: Vec<u8>| FixedSizeBinaryArray::try_from_iter(std::iter::once(v)).unwrap();
    // A 16-byte uuid from an id, non-panicking for any u64 (the tests only
    // assert on edge_id/rel_type_name, but keep the helper id-range-safe).
    let uuid = |id: u64| {
        let mut b = [0u8; 16];
        b[..8].copy_from_slice(&id.to_le_bytes());
        b.to_vec()
    };
    let ts = TimestampMicrosecondArray::from(vec![0i64]).with_timezone_opt(Some(Arc::from("UTC")));
    let batch = RecordBatch::try_new(
        TYPED_EDGE_SCHEMA.clone(),
        vec![
            Arc::new(fsb(uuid(edge_id))),
            Arc::new(fsb(uuid(src_id))),
            Arc::new(fsb(uuid(dst_id))),
            Arc::new(UInt64Array::from(vec![edge_id])),
            Arc::new(UInt64Array::from(vec![src_id])),
            Arc::new(UInt64Array::from(vec![dst_id])),
            Arc::new(ts),
        ],
    )
    .unwrap();
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, TYPED_EDGE_SCHEMA.clone(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// `(edge_id, rel_type_name)` pairs from EXPLORATORY-schema batches.
pub(super) fn edge_rel_pairs(batches: &[RecordBatch]) -> Vec<(u64, String)> {
    use arrow::array::{StringArray, UInt64Array};
    let mut out = Vec::new();
    for b in batches {
        let eids = b.column(3).as_any().downcast_ref::<UInt64Array>().unwrap();
        let rels = b
            .column_by_name("rel_type_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.push((eids.value(i), rels.value(i).to_owned()));
        }
    }
    out.sort();
    out
}

#[test]
fn read_edges_strict_wildcard_unions_all_relations() {
    let dir = TempDir::new().unwrap();
    let edges = dir.path().join("topology").join("edges");
    write_typed_edge(&edges.join("KNOWS.parquet"), 1, 1, 2);
    write_typed_edge(&edges.join("OWNS.parquet"), 2, 2, 3);

    let batches = read_edges(dir.path(), "*", OntologyMode::Strict).unwrap();
    // Union schema carries rel_type_name, each row tagged with its file stem.
    assert_eq!(batches[0].schema(), EXPLORATORY_EDGE_SCHEMA.clone());
    assert_eq!(
        edge_rel_pairs(&batches),
        vec![(1, "KNOWS".to_owned()), (2, "OWNS".to_owned())]
    );
}

#[test]
fn read_edges_strict_wildcard_empty_dir_is_one_empty_exploratory_batch() {
    let dir = TempDir::new().unwrap();
    let batches = read_edges(dir.path(), "*", OntologyMode::Strict).unwrap();
    assert_eq!(row_count(&batches), 0);
    assert_eq!(batches[0].schema(), EXPLORATORY_EDGE_SCHEMA.clone());
}

#[test]
fn read_nodes_returns_rows_and_empty_when_absent() {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("topology")).unwrap();

    // Missing file first → empty batch with the topology schema.
    let empty = read_nodes(dir.path()).unwrap();
    assert_eq!(row_count(&empty), 0);
    assert_eq!(empty[0].schema(), TOPOLOGY_NODES_SCHEMA.clone());

    // Then write one node and read it back.
    write_nodes_parquet(&dir.path().join("topology").join("nodes.parquet"));
    let batches = read_nodes(dir.path()).unwrap();
    assert_eq!(row_count(&batches), 1);
    assert_eq!(batches[0].schema(), TOPOLOGY_NODES_SCHEMA.clone());
}

#[test]
fn catalog_and_schema_debug_identity_are_stable_and_content_free() {
    let schema = GraphSchema::new();
    assert_eq!(format!("{schema:?}"), "GraphSchema { table_names: [] }");
    let schema_provider: Arc<dyn SchemaProvider> = Arc::new(schema);
    assert!(schema_provider.downcast_ref::<GraphSchema>().is_some());
    assert!(!schema_provider.table_exist("missing"));

    let dir = TempDir::new().unwrap();
    let catalog = GraphCatalog::open(dir.path(), None, &RuntimeCatalog::new()).unwrap();
    assert_eq!(
        format!("{catalog:?}"),
        "GraphCatalog { schema_names: [\"graph\"] }"
    );
    let catalog_provider: Arc<dyn CatalogProvider> = Arc::new(catalog);
    assert!(catalog_provider.downcast_ref::<GraphCatalog>().is_some());
    assert!(catalog_provider.schema("graph").is_some());
    assert!(catalog_provider.schema("private").is_none());
}

#[test]
fn checked_topology_identity_preserves_primary_absence_independently_of_membership() {
    use arrow::array::ListArray;
    use arrow::datatypes::UInt32Type;
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("nodes.parquet");
    write_nodes_parquet(&path);
    let original = read_parquet_or_empty(&path, TOPOLOGY_NODES_SCHEMA.clone())
        .unwrap()
        .remove(0);
    let normalized = normalize_topology_nodes(vec![original]).unwrap().remove(0);
    let primary_index = normalized.schema().index_of("type_id").unwrap();
    let membership_index = normalized.schema().index_of("type_ids").unwrap();
    let make = |primary: u32, members: &[u32]| {
        let mut columns = normalized.columns().to_vec();
        columns[primary_index] = Arc::new(UInt32Array::from(vec![primary]));
        let values = ListArray::from_iter_primitive::<UInt32Type, _, _>([Some(
            members.iter().copied().map(Some),
        )]);
        columns[membership_index] = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::UInt32, false)),
            values.offsets().clone(),
            values.values().clone(),
            None,
        ));
        RecordBatch::try_new(normalized.schema(), columns).unwrap()
    };
    for batch in [
        make(u32::MAX, &[]),
        make(u32::MAX, &[1073741824]),
        make(7, &[]),
        make(7, &[9]),
    ] {
        assert_eq!(
            normalize_topology_nodes(vec![batch.clone()]).unwrap(),
            vec![batch]
        );
    }
    for batch in [
        make(2147483648, &[]),
        make(3221225472, &[]),
        make(u32::MAX, &[u32::MAX]),
        make(7, &[2147483648]),
    ] {
        assert!(normalize_topology_nodes(vec![batch]).is_err());
    }
    let modern = make(u32::MAX, &[]);
    let indices = (0..modern.num_columns())
        .filter(|index| *index != membership_index)
        .collect::<Vec<_>>();
    let legacy = modern.project(&indices).unwrap();
    let decoded = normalize_topology_nodes(vec![legacy]).unwrap().remove(0);
    assert_eq!(decoded, modern);
}

#[test]
fn wave12_legacy_normalization_rejects_missing_and_mistyped_primary_labels() {
    let missing = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
        "node_id",
        DataType::UInt64,
        false,
    )])));
    assert!(normalize_topology_nodes(vec![missing]).is_err());

    let wrong_type = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "type_id",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["label"]))],
    )
    .unwrap();
    assert!(normalize_topology_nodes(vec![wrong_type]).is_err());
}

#[test]
fn wave12_typed_relation_names_are_confined_to_one_plain_stem() {
    let dir = TempDir::new().unwrap();
    for invalid in ["../escape", "nested/name", "."] {
        assert!(read_edges(dir.path(), invalid, OntologyMode::Strict).is_err());
        assert!(
            read_edges_filtered(
                dir.path(),
                invalid,
                OntologyMode::Advisory,
                &[1].into_iter().collect(),
            )
            .is_err()
        );
    }
}

#[test]
fn wave12_max_edge_id_ignores_non_parquet_but_rejects_corrupt_canonical_shards() {
    let dir = TempDir::new().unwrap();
    let edges = dir.path().join("topology/edges");
    std::fs::create_dir_all(&edges).unwrap();
    std::fs::write(edges.join("note.txt"), b"not parquet").unwrap();
    std::fs::write(edges.join("broken.parquet"), b"not parquet").unwrap();
    assert!(max_edge_id(dir.path()).is_err());
}
