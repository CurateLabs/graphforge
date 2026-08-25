//! Integration tests for [`GraphWriter`] (#579): write → flush → reload through
//! the public `GraphCatalog` reader (or, for the not-yet-registered `_untyped`
//! property file, a direct Parquet read).

use std::fs::File;
use std::sync::Arc;

use arrow::array::Array;
use datafusion::prelude::SessionContext;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tempfile::TempDir;

use graphforge_core::uuid::new_v7;
use graphforge_core::{OntologyMode, TypeId};
use graphforge_ir::{IrLiteral, RuntimeCatalog};
use graphforge_storage::{
    EdgePropertyTable, GraphCatalog, GraphWriter, PropertyTable, read_topology_generation,
};

/// Fixed timestamp so written Parquet is deterministic.
const TS: i64 = 1_700_000_000_000_000;

/// Sum the row counts across a SQL query's result batches.
async fn count(ctx: &SessionContext, sql: &str) -> usize {
    let df = ctx.sql(sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum()
}

#[tokio::test]
async fn strict_mode_round_trip_nodes_and_edges() {
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();

    // 3 Person nodes.
    let a = new_v7();
    let b = new_v7();
    let c = new_v7();
    w.create_node(a, TypeId(0)).unwrap();
    w.create_node(b, TypeId(0)).unwrap();
    w.create_node(c, TypeId(0)).unwrap();

    // 2 KNOWS edges: a->b, b->c. Surrogate edge_ids should be 1 then 2.
    assert_eq!(w.create_edge(new_v7(), "KNOWS", &a, &b).unwrap(), 1);
    assert_eq!(w.create_edge(new_v7(), "KNOWS", &b, &c).unwrap(), 2);

    w.flush().unwrap();

    // Files landed where the reader expects them.
    assert!(dir.path().join("topology/nodes.parquet").exists());
    assert!(dir.path().join("topology/edges/KNOWS.parquet").exists());

    // Reload. GraphCatalog registers `edges_<name>` from the runtime catalog's
    // relation types, so intern KNOWS first.
    let mut rc = RuntimeCatalog::new();
    rc.intern_relation_type("KNOWS");
    let gc = GraphCatalog::open(dir.path(), None, &rc).unwrap();
    let ctx = SessionContext::new();
    ctx.register_catalog("graph", Arc::new(gc));

    assert_eq!(
        count(&ctx, "SELECT node_id FROM graph.graph.topology_nodes").await,
        3
    );
    // Quote the table identifier: DataFusion lowercases unquoted identifiers,
    // but the table is registered as `edges_KNOWS` (preserving the rel type case).
    assert_eq!(
        count(&ctx, r#"SELECT edge_id FROM graph.graph."edges_KNOWS""#).await,
        2
    );
}

#[tokio::test]
async fn exploratory_mode_routes_to_catch_all_files() {
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();

    // Node with an unknown label + untyped properties.
    let src = new_v7();
    let dst = new_v7();
    w.create_node(src, TypeId(0)).unwrap();
    w.create_node(dst, TypeId(0)).unwrap();
    w.set_properties(
        &src,
        None,
        std::collections::HashMap::from([
            ("name".to_owned(), IrLiteral::Str("alice".to_owned())),
            ("age".to_owned(), IrLiteral::Int(30)),
        ]),
    )
    .unwrap();

    // Edge with an unknown relation type.
    w.create_edge(new_v7(), "UNKNOWN_REL", &src, &dst).unwrap();

    w.flush().unwrap();

    // Catch-all files exist.
    assert!(
        dir.path()
            .join("topology/edges/_exploratory.parquet")
            .exists()
    );
    assert!(dir.path().join("properties/_untyped.parquet").exists());

    // The exploratory edge file is auto-registered as `edges__exploratory`.
    let gc = GraphCatalog::open(dir.path(), None, &RuntimeCatalog::new()).unwrap();
    let ctx = SessionContext::new();
    ctx.register_catalog("graph", Arc::new(gc));
    let df = ctx
        .sql("SELECT rel_type_name FROM graph.graph.edges__exploratory")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total: usize = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total, 1);
    let col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(col.value(0), "UNKNOWN_REL");

    // `_untyped` properties are NOT auto-registered by GraphCatalog yet
    // (register_property_tables only covers known entity types), so read the
    // Parquet file directly to verify the round-trip.
    let path = dir.path().join("properties/_untyped.parquet");
    let file = File::open(&path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema = builder.schema().clone();
    assert!(schema.field_with_name("node_uuid").is_ok());
    assert!(schema.field_with_name("name").is_ok());
    assert!(schema.field_with_name("age").is_ok());

    let mut reader = builder.build().unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[tokio::test]
async fn localdatetime_property_round_trips_through_storage() {
    // #920: a localdatetime value stored as a property survives
    // set → flush → re-open(DECODE) → set → flush → read, as a typed
    // Struct{date: Date32, time: Time64(ns)} — and is NOT mis-decoded as a
    // duration (the high-risk struct-shape dispatch).
    use arrow::array::{Int64Array, StructArray, Time64NanosecondArray};
    let dir = TempDir::new().unwrap();
    let node = new_v7();
    let (days, nanos) = (5_393_i64, 45_074_645_876_123_i64); // 1984-10-11T12:31:14.645876123

    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    w.create_node(node, TypeId(0)).unwrap();
    w.set_properties(
        &node,
        None,
        std::collections::HashMap::from([(
            "ts".to_owned(),
            IrLiteral::LocalDateTime { days, nanos },
        )]),
    )
    .unwrap();
    w.flush().unwrap();

    // Re-open and add another property to the SAME node — this DECODES the
    // existing localdatetime struct before re-writing it.
    let mut w2 = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    w2.set_properties(
        &node,
        None,
        std::collections::HashMap::from([("n".to_owned(), IrLiteral::Int(7))]),
    )
    .unwrap();
    w2.flush().unwrap();

    // Read back: `ts` is a Struct{date, time} carrying the original values.
    let batch = read_property_batch(dir.path(), "_untyped").await;
    let ts = batch.column_by_name("ts").expect("ts column");
    let s = ts.as_any().downcast_ref::<StructArray>().expect("struct");
    let date = s
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("date child");
    let time = s
        .column(1)
        .as_any()
        .downcast_ref::<Time64NanosecondArray>()
        .expect("time child");
    assert_eq!(
        date.value(0),
        days,
        "localdatetime date survives round-trip"
    );
    assert_eq!(
        time.value(0),
        nanos,
        "localdatetime time survives round-trip"
    );
}

#[tokio::test]
async fn datetime_and_time_properties_round_trip_through_storage() {
    // #920: a `datetime` (Struct{date,time,offset,zone}, zone = None ⇒ NULL child)
    // and a `localtime` (native Time64) survive set → flush → re-open(DECODE) →
    // set → flush → read, NOT mis-decoded as a duration/localdatetime.
    use arrow::array::{Int32Array, Int64Array, StringArray, StructArray, Time64NanosecondArray};
    let dir = TempDir::new().unwrap();
    let node = new_v7();
    let (days, nanos, offset) = (5_393_i64, 45_074_645_876_123_i64, -3_600_i32);

    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    w.create_node(node, TypeId(0)).unwrap();
    w.set_properties(
        &node,
        None,
        std::collections::HashMap::from([
            (
                "dt".to_owned(),
                // offset-only datetime → zone None → stored as NULL.
                IrLiteral::ZonedDateTime {
                    days,
                    nanos,
                    offset,
                    zone: None,
                },
            ),
            ("t".to_owned(), IrLiteral::Time(nanos)),
        ]),
    )
    .unwrap();
    w.flush().unwrap();

    // Re-open + add a property → DECODES the datetime + time before re-writing.
    let mut w2 = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    w2.set_properties(
        &node,
        None,
        std::collections::HashMap::from([("n".to_owned(), IrLiteral::Int(7))]),
    )
    .unwrap();
    w2.flush().unwrap();

    let batch = read_property_batch(dir.path(), "_untyped").await;

    // `t` is a native Time64(ns) column.
    let t = batch.column_by_name("t").expect("t column");
    let tarr = t
        .as_any()
        .downcast_ref::<Time64NanosecondArray>()
        .expect("time64");
    assert_eq!(tarr.value(0), nanos);

    // `dt` is a Struct{date, time, offset, zone}; zone is NULL (offset-only).
    let dt = batch.column_by_name("dt").expect("dt column");
    let s = dt.as_any().downcast_ref::<StructArray>().expect("struct");
    assert_eq!(
        s.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        days
    );
    assert_eq!(
        s.column(1)
            .as_any()
            .downcast_ref::<Time64NanosecondArray>()
            .unwrap()
            .value(0),
        nanos
    );
    assert_eq!(
        s.column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        offset
    );
    let zone = s.column(3).as_any().downcast_ref::<StringArray>().unwrap();
    assert!(zone.is_null(0), "offset-only datetime stores a NULL zone");
}

#[tokio::test]
async fn list_of_temporals_property_round_trips_through_storage() {
    // #1006: a property whose value is a homogeneous LIST of temporals survives
    // set → flush → re-open(DECODE) → set → flush → read, as a typed
    // List<Struct{epoch_day}> — exercising the IrLiteral::List build + the decode
    // arm (which recurses on the element type). Dates are wide i64-days structs
    // (#1011).
    use arrow::array::{Int64Array, ListArray, StructArray};
    let dir = TempDir::new().unwrap();
    let node = new_v7();
    let (d0, d1) = (5_393_i64, 5_394_i64);

    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    w.create_node(node, TypeId(0)).unwrap();
    w.set_properties(
        &node,
        None,
        std::collections::HashMap::from([(
            "dates".to_owned(),
            IrLiteral::List(vec![IrLiteral::Date(d0), IrLiteral::Date(d1)]),
        )]),
    )
    .unwrap();
    w.flush().unwrap();

    // Re-open + add a property → DECODES the list before re-writing it.
    let mut w2 = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    w2.set_properties(
        &node,
        None,
        std::collections::HashMap::from([("n".to_owned(), IrLiteral::Int(7))]),
    )
    .unwrap();
    w2.flush().unwrap();

    // Read back: `dates` is a List<Struct{epoch_day}> with the original element
    // values (i64 days).
    let batch = read_property_batch(dir.path(), "_untyped").await;
    let col = batch.column_by_name("dates").expect("dates column");
    let list = col.as_any().downcast_ref::<ListArray>().expect("list");
    let elems = list.value(0);
    let dates = elems
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("date elements");
    assert_eq!(dates.len(), 2, "list length survives round-trip");
    let epoch_day = dates
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("epoch_day child");
    assert_eq!(epoch_day.value(0), d0);
    assert_eq!(epoch_day.value(1), d1);
}

// ---------------------------------------------------------------------------
// Append / merge across separate write sessions (#733)
// ---------------------------------------------------------------------------

/// Read `properties/<stem>.parquet` into its single batch (panics if absent).
async fn read_property_batch(dir: &std::path::Path, stem: &str) -> arrow::array::RecordBatch {
    let ctx = SessionContext::new();
    ctx.register_table(
        "properties",
        Arc::new(PropertyTable::open_discovered(dir, stem)),
    )
    .unwrap();
    let batches = ctx
        .sql("SELECT * FROM properties")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap()
}

/// Read `edge_properties/<stem>.parquet` into its single batch (panics if absent).
async fn read_edge_property_batch(dir: &std::path::Path, stem: &str) -> arrow::array::RecordBatch {
    let ctx = SessionContext::new();
    ctx.register_table(
        "edge_properties",
        Arc::new(EdgePropertyTable::open_discovered(dir, stem)),
    )
    .unwrap();
    let batches = ctx
        .sql("SELECT * FROM edge_properties")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap()
}

#[tokio::test]
async fn append_round_trip_continues_surrogates() {
    let dir = TempDir::new().unwrap();

    // Session 1: 3 nodes (ids 1,2,3) + 2 KNOWS edges (ids 1,2).
    let (a, b, c) = (new_v7(), new_v7(), new_v7());
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    assert_eq!(w.create_node(a, TypeId(0)).unwrap(), 1);
    assert_eq!(w.create_node(b, TypeId(0)).unwrap(), 2);
    assert_eq!(w.create_node(c, TypeId(0)).unwrap(), 3);
    assert_eq!(w.create_edge(new_v7(), "KNOWS", &a, &b).unwrap(), 1);
    assert_eq!(w.create_edge(new_v7(), "KNOWS", &b, &c).unwrap(), 2);
    w.flush().unwrap();

    // Session 2: REOPEN the same dir → surrogates continue from the on-disk max.
    let (d, e) = (new_v7(), new_v7());
    let mut w2 = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    assert_eq!(
        w2.create_node(d, TypeId(0)).unwrap(),
        4,
        "node ids continue"
    );
    assert_eq!(w2.create_node(e, TypeId(0)).unwrap(), 5);
    assert_eq!(
        w2.create_edge(new_v7(), "KNOWS", &d, &e).unwrap(),
        3,
        "edge ids continue"
    );
    w2.flush().unwrap();

    // Reload: both sessions' rows are present. (GraphCatalog registers
    // `edges_<rel>` for runtime relations whose typed file exists, so intern
    // KNOWS first.)
    let mut rc = RuntimeCatalog::new();
    rc.intern_relation_type("KNOWS");
    let gc = GraphCatalog::open(dir.path(), None, &rc).unwrap();
    let ctx = SessionContext::new();
    ctx.register_catalog("graph", Arc::new(gc));
    assert_eq!(
        count(&ctx, "SELECT node_id FROM graph.graph.topology_nodes").await,
        5
    );
    assert_eq!(
        count(&ctx, r#"SELECT edge_id FROM graph.graph."edges_KNOWS""#).await,
        3
    );
    // Referential integrity survived: session 2's edge (src_id=4) joins its node.
    assert_eq!(
        count(
            &ctx,
            r#"SELECT t.node_id FROM graph.graph.topology_nodes t JOIN graph.graph."edges_KNOWS" e ON t.node_id = e.src_id WHERE e.edge_id = 3"#,
        )
        .await,
        1
    );
}

#[tokio::test]
async fn append_property_merge_adds_new_column() {
    let dir = TempDir::new().unwrap();
    let a = new_v7();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    w.create_node(a, TypeId(0)).unwrap();
    w.set_properties(
        &a,
        None,
        std::collections::HashMap::from([("name".to_owned(), IrLiteral::Str("A".to_owned()))]),
    )
    .unwrap();
    w.flush().unwrap();

    let b = new_v7();
    let mut w2 = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    w2.create_node(b, TypeId(0)).unwrap();
    w2.set_properties(
        &b,
        None,
        std::collections::HashMap::from([
            ("name".to_owned(), IrLiteral::Str("B".to_owned())),
            ("age".to_owned(), IrLiteral::Int(10)),
        ]),
    )
    .unwrap();
    w2.flush().unwrap();

    let batch = read_property_batch(dir.path(), "_untyped").await;
    assert_eq!(batch.num_rows(), 2, "both rows retained");
    let age = batch
        .column_by_name("age")
        .expect("age column added on the second flush")
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(age.null_count(), 1);
    assert!((0..age.len()).any(|row| age.is_valid(row) && age.value(row) == 10));
}

#[tokio::test]
async fn append_property_merge_preserves_heterogeneous_scalar_types() {
    let dir = TempDir::new().unwrap();
    let a = new_v7();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    w.create_node(a, TypeId(0)).unwrap();
    w.set_properties(
        &a,
        None,
        std::collections::HashMap::from([("x".to_owned(), IrLiteral::Int(1))]),
    )
    .unwrap();
    w.flush().unwrap();

    let b = new_v7();
    let mut w2 = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    w2.create_node(b, TypeId(0)).unwrap();
    w2.set_properties(
        &b,
        None,
        std::collections::HashMap::from([("x".to_owned(), IrLiteral::Str("two".to_owned()))]),
    )
    .unwrap();
    w2.flush().unwrap();

    // Mixed Int (flush 1) + Str (flush 2) retain their original scalar types.
    let batch = read_property_batch(dir.path(), "_untyped").await;
    let x = batch
        .column_by_name("x")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .expect("x uses tagged heterogeneous scalars");
    let tags = x
        .column_by_name("__het_tag")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int8Array>()
        .unwrap();
    let mut observed = (0..tags.len())
        .map(|row| tags.value(row))
        .collect::<Vec<_>>();
    observed.sort_unstable();
    assert_eq!(observed, vec![0, 2]);
}

#[tokio::test]
async fn append_property_merge_null_first_across_flushes() {
    // Highest-risk path: a column that is all-null on disk must NOT pin the
    // merged column to Utf8 — a concrete value in the next flush fixes the type.
    let dir = TempDir::new().unwrap();
    let a = new_v7();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    w.create_node(a, TypeId(0)).unwrap();
    w.set_properties(
        &a,
        None,
        std::collections::HashMap::from([("score".to_owned(), IrLiteral::Null)]),
    )
    .unwrap();
    w.flush().unwrap();

    let b = new_v7();
    let mut w2 = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    w2.create_node(b, TypeId(0)).unwrap();
    w2.set_properties(
        &b,
        None,
        std::collections::HashMap::from([("score".to_owned(), IrLiteral::Int(42))]),
    )
    .unwrap();
    w2.flush().unwrap();

    let batch = read_property_batch(dir.path(), "_untyped").await;
    let score = batch
        .column_by_name("score")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .expect("cross-shard null and Int normalize to tagged scalars");
    assert_eq!(batch.num_rows(), 2);
    // Exactly one concrete value (42); the all-null first flush stays null.
    assert_eq!(score.null_count(), 1);
}

#[tokio::test]
async fn append_edge_stems_are_isolated() {
    let dir = TempDir::new().unwrap();
    let (a, b) = (new_v7(), new_v7());
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    w.create_node(a, TypeId(0)).unwrap();
    w.create_node(b, TypeId(0)).unwrap();
    w.create_edge(new_v7(), "KNOWS", &a, &b).unwrap();
    w.flush().unwrap();

    // Second session writes only LIKES — KNOWS.parquet must be left intact.
    let mut w2 = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    w2.register_existing_node(a, 1).unwrap();
    w2.register_existing_node(b, 2).unwrap();
    w2.create_edge(new_v7(), "LIKES", &a, &b).unwrap();
    w2.flush().unwrap();

    let mut rc = RuntimeCatalog::new();
    rc.intern_relation_type("KNOWS");
    rc.intern_relation_type("LIKES");
    let gc = GraphCatalog::open(dir.path(), None, &rc).unwrap();
    let ctx = SessionContext::new();
    ctx.register_catalog("graph", Arc::new(gc));
    assert_eq!(
        count(&ctx, r#"SELECT edge_id FROM graph.graph."edges_KNOWS""#).await,
        1,
        "KNOWS untouched by the LIKES-only flush"
    );
    assert_eq!(
        count(&ctx, r#"SELECT edge_id FROM graph.graph."edges_LIKES""#).await,
        1
    );
}

// ---------------------------------------------------------------------------
// Edge-property persistence (#784): mirror the node-property tests, keyed by
// `edge_uuid`, written under `edge_properties/<REL_TYPE>.parquet`.
// ---------------------------------------------------------------------------

/// Create two nodes + a KNOWS edge carrying `props`, flush, and return the
/// edge's UUID for assertions.
fn write_knows_edge_with_props(
    dir: &std::path::Path,
    mode: OntologyMode,
    props: std::collections::HashMap<String, IrLiteral>,
) {
    let (a, b) = (new_v7(), new_v7());
    let mut w = GraphWriter::open_at(dir, mode, TS).unwrap();
    w.create_node(a, TypeId(0)).unwrap();
    w.create_node(b, TypeId(0)).unwrap();
    let edge = new_v7();
    w.create_edge(edge, "KNOWS", &a, &b).unwrap();
    w.set_edge_properties(&edge, Some("KNOWS"), props).unwrap();
    w.flush().unwrap();
}

#[tokio::test]
async fn edge_property_round_trip_persists_value() {
    let dir = TempDir::new().unwrap();
    write_knows_edge_with_props(
        dir.path(),
        OntologyMode::Strict,
        std::collections::HashMap::from([("since".to_owned(), IrLiteral::Int(2020))]),
    );

    // Edge properties land in the dedicated `edge_properties/` dir, keyed by the
    // relation name — never under `properties/` (which is node-only).
    assert!(dir.path().join("edge_properties/KNOWS.parquet").exists());
    assert!(!dir.path().join("properties/KNOWS.parquet").exists());

    let batch = read_edge_property_batch(dir.path(), "KNOWS").await;
    assert_eq!(batch.num_rows(), 1);
    assert!(
        batch.schema().field_with_name("edge_uuid").is_ok(),
        "edge-property file is keyed by edge_uuid"
    );
    let since = batch
        .column_by_name("since")
        .expect("since column")
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("since is Int64");
    assert_eq!(since.value(0), 2020);
}

#[tokio::test]
async fn edge_property_routes_by_rel_name_even_in_exploratory_mode() {
    // Node properties fall back to `_untyped` in exploratory mode, but edge
    // properties always key by the relation name so the read side can resolve
    // the stem from the rel type.
    let dir = TempDir::new().unwrap();
    write_knows_edge_with_props(
        dir.path(),
        OntologyMode::Exploratory,
        std::collections::HashMap::from([("since".to_owned(), IrLiteral::Int(1999))]),
    );
    assert!(dir.path().join("edge_properties/KNOWS.parquet").exists());
    assert!(!dir.path().join("edge_properties/_untyped.parquet").exists());
}

#[tokio::test]
async fn append_edge_property_merge_adds_new_column() {
    let dir = TempDir::new().unwrap();
    write_knows_edge_with_props(
        dir.path(),
        OntologyMode::Strict,
        std::collections::HashMap::from([("since".to_owned(), IrLiteral::Int(2020))]),
    );
    // Second session: a new KNOWS edge with an extra `weight` property.
    write_knows_edge_with_props(
        dir.path(),
        OntologyMode::Strict,
        std::collections::HashMap::from([
            ("since".to_owned(), IrLiteral::Int(2021)),
            ("weight".to_owned(), IrLiteral::Float(0.5)),
        ]),
    );

    let batch = read_edge_property_batch(dir.path(), "KNOWS").await;
    assert_eq!(batch.num_rows(), 2, "both edge rows retained");
    let weight = batch
        .column_by_name("weight")
        .expect("weight column added on the second flush")
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(weight.null_count(), 1);
    assert!((0..weight.len()).any(|row| weight.is_valid(row) && weight.value(row) == 0.5));
}

#[tokio::test]
async fn append_edge_property_merge_preserves_heterogeneous_scalar_types() {
    let dir = TempDir::new().unwrap();
    write_knows_edge_with_props(
        dir.path(),
        OntologyMode::Strict,
        std::collections::HashMap::from([("x".to_owned(), IrLiteral::Int(1))]),
    );
    write_knows_edge_with_props(
        dir.path(),
        OntologyMode::Strict,
        std::collections::HashMap::from([("x".to_owned(), IrLiteral::Str("two".to_owned()))]),
    );

    let batch = read_edge_property_batch(dir.path(), "KNOWS").await;
    let x = batch
        .column_by_name("x")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .expect("x uses tagged heterogeneous scalars");
    let tags = x
        .column_by_name("__het_tag")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int8Array>()
        .unwrap();
    let mut observed = (0..tags.len())
        .map(|row| tags.value(row))
        .collect::<Vec<_>>();
    observed.sort_unstable();
    assert_eq!(observed, vec![0, 2]);
}

#[tokio::test]
async fn append_edge_property_merge_null_first_across_flushes() {
    // An all-null edge-property column on disk must not pin the merged column to
    // Utf8 — a concrete value in the next flush fixes the type (mirror of the
    // node-property null-first test).
    let dir = TempDir::new().unwrap();
    write_knows_edge_with_props(
        dir.path(),
        OntologyMode::Strict,
        std::collections::HashMap::from([("score".to_owned(), IrLiteral::Null)]),
    );
    write_knows_edge_with_props(
        dir.path(),
        OntologyMode::Strict,
        std::collections::HashMap::from([("score".to_owned(), IrLiteral::Int(42))]),
    );

    let batch = read_edge_property_batch(dir.path(), "KNOWS").await;
    let score = batch
        .column_by_name("score")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .expect("cross-shard null and Int normalize to tagged scalars");
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(score.null_count(), 1);
}

#[tokio::test]
async fn edge_property_stems_are_isolated() {
    // Two relation types' property files must not clobber each other.
    let dir = TempDir::new().unwrap();
    write_knows_edge_with_props(
        dir.path(),
        OntologyMode::Strict,
        std::collections::HashMap::from([("since".to_owned(), IrLiteral::Int(2020))]),
    );
    // Add a LIKES edge with its own property.
    let (a, b) = (new_v7(), new_v7());
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    w.create_node(a, TypeId(0)).unwrap();
    w.create_node(b, TypeId(0)).unwrap();
    let edge = new_v7();
    w.create_edge(edge, "LIKES", &a, &b).unwrap();
    w.set_edge_properties(
        &edge,
        Some("LIKES"),
        std::collections::HashMap::from([("rating".to_owned(), IrLiteral::Int(5))]),
    )
    .unwrap();
    w.flush().unwrap();

    let knows = read_edge_property_batch(dir.path(), "KNOWS").await;
    assert_eq!(knows.num_rows(), 1);
    assert!(knows.schema().field_with_name("since").is_ok());
    assert!(
        knows.schema().field_with_name("rating").is_err(),
        "KNOWS file must not gain LIKES' column"
    );
    let likes = read_edge_property_batch(dir.path(), "LIKES").await;
    assert_eq!(likes.num_rows(), 1);
    assert!(likes.schema().field_with_name("rating").is_ok());
}

// ---------------------------------------------------------------------------
// topology_generation bumping (#759)
// ---------------------------------------------------------------------------

#[test]
fn flush_bumps_topology_generation_once_per_topology_flush() {
    let dir = TempDir::new().unwrap();
    assert_eq!(read_topology_generation(dir.path()).unwrap(), 0);

    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    let (a, b) = (new_v7(), new_v7());
    w.create_node(a, TypeId(0)).unwrap();
    w.create_node(b, TypeId(0)).unwrap();
    w.create_edge(new_v7(), "KNOWS", &a, &b).unwrap();
    w.flush().unwrap();
    assert_eq!(
        read_topology_generation(dir.path()).unwrap(),
        1,
        "one bump per committed batch, however many topology files it staged"
    );

    w.create_node(new_v7(), TypeId(0)).unwrap();
    w.flush().unwrap();
    assert_eq!(read_topology_generation(dir.path()).unwrap(), 2);
}

#[test]
fn property_only_flush_does_not_bump_topology_generation() {
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    let a = new_v7();
    w.create_node(a, TypeId(0)).unwrap();
    w.flush().unwrap();
    assert_eq!(read_topology_generation(dir.path()).unwrap(), 1);

    // A second flush staging only a property file must not bump: properties
    // cannot change adjacency.
    w.set_properties(
        &a,
        None,
        std::collections::HashMap::from([("name".to_owned(), IrLiteral::Str("A".into()))]),
    )
    .unwrap();
    w.flush().unwrap();
    assert!(dir.path().join("properties/_untyped.parquet").exists());
    assert_eq!(read_topology_generation(dir.path()).unwrap(), 1);

    // An empty flush stages nothing and must not bump either.
    w.flush().unwrap();
    assert_eq!(read_topology_generation(dir.path()).unwrap(), 1);
}
