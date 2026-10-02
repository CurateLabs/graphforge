use super::super::tests::row_count;
use super::super::tests::write_edge_parquet;
use super::super::tests::write_nodes_parquet;
use super::super::tests::write_nodes_parquet_value;
use super::super::tests::write_typed_edge;
use super::EdgePropertyTable;
use super::PropertyTable;
use super::TopologyNodeTable;
use super::TypedEdgeTable;
use super::UnionEdgeTable;
use crate::schemas::EXPLORATORY_EDGE_SCHEMA;
use crate::schemas::TYPED_EDGE_SCHEMA;
use crate::schemas::property_schema;
use arrow::array::FixedSizeBinaryArray;
use arrow::array::RecordBatch;
use arrow::array::TimestampMicrosecondArray;
use arrow::array::UInt64Array;
use datafusion::datasource::TableProvider;
use datafusion::datasource::TableType;
use datafusion::prelude::SessionContext;
use datafusion_catalog::Session;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn topology_node_table_scan_returns_rows() {
    let dir = TempDir::new().unwrap();
    let nodes_dir = dir.path().join("topology");
    std::fs::create_dir_all(&nodes_dir).unwrap();
    let path = nodes_dir.join("nodes.parquet");
    write_nodes_parquet(&path);

    let table = TopologyNodeTable::new(path);
    let ctx = SessionContext::new();
    ctx.register_table("nodes", Arc::new(table)).unwrap();
    let df = ctx.sql("SELECT node_id FROM nodes").await.unwrap();
    let batches = df.collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1);
}

#[tokio::test]
async fn topology_node_table_scan_unions_mixed_layout() {
    let dir = TempDir::new().unwrap();
    write_nodes_parquet_value(&dir.path().join("topology/nodes.parquet"), 1, 1);
    write_nodes_parquet_value(
        &dir.path()
            .join("topology/nodes/00000000000000000002-00000000000000000002.parquet"),
        2,
        2,
    );
    let table = TopologyNodeTable::open_project(dir.path()).unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("nodes", Arc::new(table)).unwrap();
    let batches = ctx
        .sql("SELECT node_id FROM nodes ORDER BY node_id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
}

#[tokio::test]
async fn topology_node_table_missing_file_returns_empty() {
    let table = TopologyNodeTable::new(PathBuf::from("/nonexistent/nodes.parquet"));
    let ctx = SessionContext::new();
    ctx.register_table("nodes", Arc::new(table)).unwrap();
    let df = ctx.sql("SELECT node_id FROM nodes").await.unwrap();
    let batches = df.collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 0);
}

#[tokio::test]
async fn typed_edge_table_scan_returns_rows() {
    let dir = TempDir::new().unwrap();
    let edge_path = dir
        .path()
        .join("topology")
        .join("edges")
        .join("KNOWS.parquet");
    write_edge_parquet(&edge_path);

    let table = TypedEdgeTable::open(dir.path(), "KNOWS");
    let ctx = SessionContext::new();
    ctx.register_table("edges", Arc::new(table)).unwrap();
    let df = ctx.sql("SELECT src_id, dst_id FROM edges").await.unwrap();
    let batches = df.collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1);
}

#[tokio::test]
async fn typed_edge_table_exploratory_has_rel_type_name_column() {
    let table = TypedEdgeTable::open(Path::new("/nonexistent"), "_exploratory");
    let schema = table.schema();
    assert!(
        schema.field_with_name("rel_type_name").is_ok(),
        "exploratory schema must have rel_type_name"
    );
}

#[tokio::test]
async fn union_edge_table_scan_unions_all_relations() {
    let dir = TempDir::new().unwrap();
    let edges = dir.path().join("topology").join("edges");
    write_typed_edge(&edges.join("KNOWS.parquet"), 1, 1, 2);
    write_typed_edge(&edges.join("OWNS.parquet"), 2, 2, 3);

    let table = UnionEdgeTable::open(dir.path());
    assert_eq!(table.schema(), EXPLORATORY_EDGE_SCHEMA.clone());
    let ctx = SessionContext::new();
    ctx.register_table("edges", Arc::new(table)).unwrap();
    let df = ctx
        .sql("SELECT edge_id, rel_type_name FROM edges ORDER BY edge_id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(row_count(&batches), 2, "both relations' edges unioned");
}

#[tokio::test]
async fn property_table_missing_file_returns_empty_with_correct_schema() {
    let schema = Arc::new(property_schema("Person", &[]));
    let table = PropertyTable::open(Path::new("/nonexistent"), "Person", schema.clone());
    let ctx = SessionContext::new();
    ctx.register_table("props", Arc::new(table)).unwrap();
    let df = ctx.sql("SELECT node_uuid FROM props").await.unwrap();
    let batches = df.collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 0);
}

#[tokio::test]
async fn every_table_provider_exposes_base_contract_and_empty_scan() {
    let dir = TempDir::new().unwrap();
    let providers: Vec<Arc<dyn TableProvider>> = vec![
        Arc::new(TopologyNodeTable::new(
            dir.path().join("topology/nodes.parquet"),
        )),
        Arc::new(TypedEdgeTable::open(dir.path(), "KNOWS")),
        Arc::new(UnionEdgeTable::open(dir.path())),
        Arc::new(PropertyTable::open_discovered(dir.path(), "Person").unwrap()),
        Arc::new(EdgePropertyTable::open_discovered(dir.path(), "KNOWS").unwrap()),
    ];
    let ctx = SessionContext::new();
    for (index, provider) in providers.into_iter().enumerate() {
        assert_eq!(provider.table_type(), TableType::Base);
        assert!(
            provider.is::<TopologyNodeTable>()
                || provider.is::<TypedEdgeTable>()
                || provider.is::<UnionEdgeTable>()
                || provider.is::<PropertyTable>()
                || provider.is::<EdgePropertyTable>()
        );
        let name = format!("provider_{index}");
        let expected = provider.schema();
        ctx.register_table(&name, provider).unwrap();
        let frame = ctx.sql(&format!("SELECT * FROM {name}")).await.unwrap();
        assert_eq!(frame.schema().inner(), &expected);
        let batches = frame.collect().await.unwrap();
        assert_eq!(row_count(&batches), 0);
    }
}

#[tokio::test]
async fn query_providers_build_streaming_parquet_plan_not_memtable() {
    let dir = TempDir::new().unwrap();
    let nodes = dir.path().join("topology/nodes.parquet");
    std::fs::create_dir_all(nodes.parent().unwrap()).unwrap();
    write_nodes_parquet(&nodes);
    let edges = dir.path().join("topology/edges/KNOWS.parquet");
    write_edge_parquet(&edges);

    let ctx = SessionContext::new();
    let state = ctx.state();
    let cases: Vec<(&str, Arc<dyn TableProvider>)> = vec![
        ("nodes", Arc::new(TopologyNodeTable::new(nodes.clone()))),
        ("edges", Arc::new(TypedEdgeTable::open(dir.path(), "KNOWS"))),
        ("union", Arc::new(UnionEdgeTable::open(dir.path()))),
        (
            "props",
            Arc::new(PropertyTable::open_discovered(dir.path(), "Person").unwrap()),
        ),
        (
            "edge_props",
            Arc::new(EdgePropertyTable::open_discovered(dir.path(), "KNOWS").unwrap()),
        ),
    ];
    for (label, provider) in cases {
        let plan = provider
            .scan(&state as &dyn Session, None, &[], None)
            .await
            .unwrap();
        let text = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(false)
            .to_string();
        let expected = if matches!(label, "props" | "edge_props") {
            "PropertyOverlayExec"
        } else {
            "GraphForgeParquetExec"
        };
        assert!(
            text.contains(expected),
            "{label}: expected {expected}, got:\n{text}"
        );
        assert!(
            !text.contains("MemoryExec") && !text.contains("MemTable"),
            "{label}: MemTable/MemoryExec must not appear:\n{text}"
        );
    }
    // Corrupt after scan planning — execute must fail closed with a structured error.
    let table = TypedEdgeTable::open(dir.path(), "KNOWS");
    let plan = table
        .scan(&state as &dyn Session, None, &[], None)
        .await
        .unwrap();
    std::fs::write(&edges, b"not-parquet").unwrap();
    let err = datafusion::physical_plan::collect(plan, ctx.task_ctx())
        .await
        .unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("parquet")
            || err.to_string().to_lowercase().contains("corrupt"),
        "structured failure, got: {err}"
    );
}

#[tokio::test]
async fn provider_scan_honors_session_batch_size_policy() {
    let dir = TempDir::new().unwrap();
    let path = dir
        .path()
        .join("topology")
        .join("edges")
        .join("KNOWS.parquet");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let n = 20usize;
    let uuids = FixedSizeBinaryArray::try_from_iter((0..n).map(|i| {
        let mut bytes = vec![0u8; 16];
        bytes[15] = i as u8;
        bytes
    }))
    .unwrap();
    let src = FixedSizeBinaryArray::try_from_iter((0..n).map(|_| vec![1u8; 16])).unwrap();
    let dst = FixedSizeBinaryArray::try_from_iter((0..n).map(|_| vec![2u8; 16])).unwrap();
    let ts =
        TimestampMicrosecondArray::from(vec![0i64; n]).with_timezone_opt(Some(Arc::from("UTC")));
    let batch = RecordBatch::try_new(
        TYPED_EDGE_SCHEMA.clone(),
        vec![
            Arc::new(uuids),
            Arc::new(src),
            Arc::new(dst),
            Arc::new(UInt64Array::from((1..=n as u64).collect::<Vec<_>>())),
            Arc::new(UInt64Array::from(vec![1u64; n])),
            Arc::new(UInt64Array::from(vec![2u64; n])),
            Arc::new(ts),
        ],
    )
    .unwrap();
    let file = File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(
        file,
        TYPED_EDGE_SCHEMA.clone(),
        Some(WriterProperties::builder().build()),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let config = datafusion::prelude::SessionConfig::new().with_batch_size(5);
    let state = datafusion::execution::SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .build();
    let ctx = SessionContext::new_with_state(state);
    let table = TypedEdgeTable::open(dir.path(), "KNOWS");
    let session = ctx.state();
    let plan = table
        .scan(&session as &dyn Session, None, &[], None)
        .await
        .unwrap();
    let display = datafusion::physical_plan::displayable(plan.as_ref())
        .one_line()
        .to_string();
    assert!(
        display.contains("batch_size=5"),
        "policy batch size must appear on plan: {display}"
    );
    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
        .await
        .unwrap();
    assert!(
        batches.len() >= 2,
        "expected multiple batches, got {}",
        batches.len()
    );
    assert!(batches.iter().all(|b| b.num_rows() <= 5));
    assert_eq!(row_count(&batches), 20);
}

/// A topology entry for a file beneath `root`, admitted by length and XXH64.
fn topology_entry(root: &Path, relative: &str) -> crate::GraphFileEntry {
    let bytes = std::fs::read(root.join(relative)).unwrap();
    crate::GraphFileEntry {
        relative_path: relative.to_owned(),
        byte_length: bytes.len() as u64,
        content_sha256: "0".repeat(64),
        content_xxh64: crate::corruption_checksum::checksum(&bytes),
        role: crate::GraphFileRole::Topology,
    }
}

async fn node_ids(table: TopologyNodeTable) -> Vec<u64> {
    let ctx = SessionContext::new();
    ctx.register_table("nodes", Arc::new(table)).unwrap();
    let batches = ctx
        .sql("SELECT node_id FROM nodes ORDER BY node_id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            (0..batch.num_rows()).map(move |row| ids.value(row))
        })
        .collect()
}

/// #1388: the node table lists what the inventory declares, never the
/// directory; an undeclared file in `topology/nodes/` is not read. Without an
/// inventory the directory listing remains (expanded generations verify it
/// at open).
#[tokio::test]
async fn open_with_inventory_reads_only_declared_node_files() {
    let dir = TempDir::new().unwrap();
    let declared = "topology/nodes/00000000000000000001-00000000000000000001.parquet";
    let planted = "topology/nodes/00000000000000000002-00000000000000000002.parquet";
    write_nodes_parquet_value(&dir.path().join(declared), 1, 1);
    write_nodes_parquet_value(&dir.path().join(planted), 2, 2);
    let inventory = crate::AuthenticatedPropertyInventory::from_entries_at_root(
        dir.path(),
        vec![topology_entry(dir.path(), declared)],
    )
    .unwrap();
    assert_eq!(
        inventory.node_fragments().unwrap(),
        vec![(dir.path().join(declared), declared.to_owned())]
    );
    let table = TopologyNodeTable::open_with_inventory(dir.path(), Some(&inventory)).unwrap();
    assert_eq!(node_ids(table).await, vec![1]);
    let listed = TopologyNodeTable::open_with_inventory(dir.path(), None).unwrap();
    assert_eq!(node_ids(listed).await, vec![1, 2]);
}

/// The declared set keeps the legacy flat file first, then shards by range,
/// and refuses what `node_parquet_files` would refuse of a listing.
#[test]
fn declared_node_files_are_ordered_and_validated_like_a_listing() {
    let dir = TempDir::new().unwrap();
    let legacy = "topology/nodes.parquet";
    let shard = "topology/nodes/00000000000000000002-00000000000000000003.parquet";
    write_nodes_parquet_value(&dir.path().join(legacy), 1, 1);
    write_nodes_parquet_value(&dir.path().join(shard), 2, 2);
    let inventory = crate::AuthenticatedPropertyInventory::from_entries_at_root(
        dir.path(),
        vec![
            topology_entry(dir.path(), shard),
            topology_entry(dir.path(), legacy),
        ],
    )
    .unwrap();
    let fragments = inventory.node_fragments().unwrap();
    assert_eq!(
        fragments
            .iter()
            .map(|(_, relative)| relative.as_str())
            .collect::<Vec<_>>(),
        vec![legacy, shard]
    );

    // A staged temporary or any non-Parquet name beside the shards is
    // ignored, as the listing ignores it; a Parquet file with a non-canonical
    // name or an overlapping range is refused.
    for ignored in [
        "topology/nodes/00000000000000000002-00000000000000000003.parquet.a1b2c3.tmp",
        "topology/nodes/00000000000000000005-00000000000000000006.arrow",
    ] {
        write_nodes_parquet_value(&dir.path().join(ignored), 2, 2);
        let inventory = crate::AuthenticatedPropertyInventory::from_entries_at_root(
            dir.path(),
            vec![
                topology_entry(dir.path(), shard),
                topology_entry(dir.path(), ignored),
            ],
        )
        .unwrap();
        assert_eq!(inventory.node_fragments().unwrap().len(), 1, "{ignored}");
    }
    for (name, message) in [
        ("topology/nodes/nodes-extra.parquet", "canonical"),
        (
            "topology/nodes/00000000000000000003-00000000000000000004.parquet",
            "overlap",
        ),
    ] {
        write_nodes_parquet_value(&dir.path().join(name), 3, 3);
        let error = crate::AuthenticatedPropertyInventory::from_entries_at_root(
            dir.path(),
            vec![
                topology_entry(dir.path(), shard),
                topology_entry(dir.path(), name),
            ],
        )
        .unwrap_err();
        assert!(error.to_string().contains(message), "{name}: {error}");
    }
}
