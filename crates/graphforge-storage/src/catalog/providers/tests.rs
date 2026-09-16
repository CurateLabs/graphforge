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
        Arc::new(PropertyTable::open_discovered(dir.path(), "Person")),
        Arc::new(EdgePropertyTable::open_discovered(dir.path(), "KNOWS")),
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
            Arc::new(PropertyTable::open_discovered(dir.path(), "Person")),
        ),
        (
            "edge_props",
            Arc::new(EdgePropertyTable::open_discovered(dir.path(), "KNOWS")),
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
