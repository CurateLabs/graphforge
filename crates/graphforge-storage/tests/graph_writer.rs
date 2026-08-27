//! Integration tests for [`GraphWriter`] (#579): write → flush → reload through
//! the public `GraphCatalog` reader and authenticated immutable property
//! authority.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, FixedSizeBinaryArray, Int64Array, StringArray};
use datafusion::prelude::SessionContext;
use tempfile::TempDir;

use graphforge_core::uuid::new_v7;
use graphforge_core::{OntologyMode, TypeId};
use graphforge_ir::{IrLiteral, RuntimeCatalog};
use graphforge_storage::{
    EdgePropertyTable, GraphCatalog, GraphWriter, PropertyOverlayLimits, PropertyRouteKind,
    PropertySnapshotRow, PropertyTable, delete_edges, enumerate_property_fragments,
    read_topology_generation, remove_edge_properties, set_edge_properties_rewrite,
    visit_authenticated_property_snapshots,
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
    assert_canonical_fragments(dir.path(), PropertyRouteKind::Node, "_untyped", 1);

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

    let schema = PropertyTable::open_discovered(dir.path(), "_untyped").schema_ref();
    assert!(schema.field_with_name("node_uuid").is_ok());
    assert!(schema.field_with_name("name").is_ok());
    assert!(schema.field_with_name("age").is_ok());

    let rows = read_property_rows(dir.path(), "_untyped");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[src.as_bytes()].values["name"],
        IrLiteral::Str("alice".into())
    );
    assert_eq!(rows[src.as_bytes()].values["age"], IrLiteral::Int(30));
}

#[tokio::test]
async fn localdatetime_property_round_trips_through_storage() {
    // #920: a localdatetime value stored as a property survives
    // set → flush → re-open(DECODE) → set → flush → read, as a typed
    // Struct{date: Date32, time: Time64(ns)} — and is NOT mis-decoded as a
    // duration (the high-risk struct-shape dispatch).
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

    let rows = read_property_rows(dir.path(), "_untyped");
    assert_eq!(
        rows[node.as_bytes()].values["ts"],
        IrLiteral::LocalDateTime { days, nanos }
    );
    assert_eq!(rows[node.as_bytes()].values["n"], IrLiteral::Int(7));
}

#[tokio::test]
async fn datetime_and_time_properties_round_trip_through_storage() {
    // #920: a `datetime` (Struct{date,time,offset,zone}, zone = None ⇒ NULL child)
    // and a `localtime` (native Time64) survive set → flush → re-open(DECODE) →
    // set → flush → read, NOT mis-decoded as a duration/localdatetime.
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

    let rows = read_property_rows(dir.path(), "_untyped");
    assert_eq!(rows[node.as_bytes()].values["t"], IrLiteral::Time(nanos));
    assert_eq!(
        rows[node.as_bytes()].values["dt"],
        IrLiteral::ZonedDateTime {
            days,
            nanos,
            offset,
            zone: None,
        }
    );
}

#[tokio::test]
async fn list_of_temporals_property_round_trips_through_storage() {
    // #1006: a property whose value is a homogeneous LIST of temporals survives
    // set → flush → re-open(DECODE) → set → flush → read, as a typed
    // List<Struct{epoch_day}> — exercising the IrLiteral::List build + the decode
    // arm (which recurses on the element type). Dates are wide i64-days structs
    // (#1011).
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

    let rows = read_property_rows(dir.path(), "_untyped");
    assert_eq!(
        rows[node.as_bytes()].values["dates"],
        IrLiteral::List(vec![IrLiteral::Date(d0), IrLiteral::Date(d1)])
    );
}

// ---------------------------------------------------------------------------
// Append / merge across separate write sessions (#733)
// ---------------------------------------------------------------------------

fn read_rows(
    dir: &std::path::Path,
    kind: PropertyRouteKind,
    route: &str,
) -> BTreeMap<[u8; 16], PropertySnapshotRow> {
    let scratch = TempDir::new().unwrap();
    let mut rows = BTreeMap::new();
    visit_authenticated_property_snapshots(
        dir,
        kind,
        route,
        scratch.path(),
        PropertyOverlayLimits::default(),
        |row| {
            assert!(!row.tombstone, "logical visitor must omit tombstones");
            assert!(rows.insert(row.uuid, row).is_none(), "UUIDs are unique");
            Ok(())
        },
    )
    .unwrap();
    rows
}

fn read_property_rows(
    dir: &std::path::Path,
    route: &str,
) -> BTreeMap<[u8; 16], PropertySnapshotRow> {
    read_rows(dir, PropertyRouteKind::Node, route)
}

fn read_edge_property_rows(
    dir: &std::path::Path,
    route: &str,
) -> BTreeMap<[u8; 16], PropertySnapshotRow> {
    read_rows(dir, PropertyRouteKind::Edge, route)
}

fn assert_canonical_fragments(
    dir: &std::path::Path,
    kind: PropertyRouteKind,
    route: &str,
    minimum: usize,
) {
    let fragments = enumerate_property_fragments(dir, kind, route).unwrap();
    assert!(fragments.len() >= minimum, "missing immutable fragments");
    for fragment in fragments {
        assert_ne!(fragment.id.generation, 0, "new writes are not legacy files");
        assert_eq!(
            fragment.path.file_name().unwrap().to_str().unwrap(),
            fragment.id.file_name()
        );
        assert_eq!(fragment.path.parent().unwrap().file_name().unwrap(), route);
    }
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

    let rows = read_property_rows(dir.path(), "_untyped");
    assert_eq!(rows.len(), 2, "both rows retained");
    assert!(!rows[a.as_bytes()].values.contains_key("age"));
    assert_eq!(rows[b.as_bytes()].values["age"], IrLiteral::Int(10));
    assert_eq!(
        rows[a.as_bytes()].values["name"],
        IrLiteral::Str("A".into())
    );
    assert_eq!(
        rows[b.as_bytes()].values["name"],
        IrLiteral::Str("B".into())
    );
    assert_canonical_fragments(dir.path(), PropertyRouteKind::Node, "_untyped", 2);
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

    // Mixed Int (flush 1) + Str (flush 2) retain their logical scalar types.
    let rows = read_property_rows(dir.path(), "_untyped");
    assert_eq!(rows[a.as_bytes()].values["x"], IrLiteral::Int(1));
    assert_eq!(rows[b.as_bytes()].values["x"], IrLiteral::Str("two".into()));
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

    let rows = read_property_rows(dir.path(), "_untyped");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[a.as_bytes()].values["score"], IrLiteral::Null);
    assert_eq!(rows[b.as_bytes()].values["score"], IrLiteral::Int(42));
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
    w2.register_existing_endpoints(&[a, b]).unwrap();
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
) -> uuid::Uuid {
    let (a, b) = (new_v7(), new_v7());
    let mut w = GraphWriter::open_at(dir, mode, TS).unwrap();
    w.create_node(a, TypeId(0)).unwrap();
    w.create_node(b, TypeId(0)).unwrap();
    let edge = new_v7();
    w.create_edge(edge, "KNOWS", &a, &b).unwrap();
    w.set_edge_properties(&edge, Some("KNOWS"), props).unwrap();
    w.flush().unwrap();
    edge
}

#[tokio::test]
async fn edge_property_round_trip_persists_value() {
    let dir = TempDir::new().unwrap();
    let edge = write_knows_edge_with_props(
        dir.path(),
        OntologyMode::Strict,
        std::collections::HashMap::from([("since".to_owned(), IrLiteral::Int(2020))]),
    );

    // Edge properties land in the dedicated `edge_properties/` dir, keyed by the
    // relation name — never under `properties/` (which is node-only).
    assert_canonical_fragments(dir.path(), PropertyRouteKind::Edge, "KNOWS", 1);
    assert!(
        enumerate_property_fragments(dir.path(), PropertyRouteKind::Node, "KNOWS")
            .unwrap()
            .is_empty()
    );

    let rows = read_edge_property_rows(dir.path(), "KNOWS");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[edge.as_bytes()].values["since"], IrLiteral::Int(2020));
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
    assert_canonical_fragments(dir.path(), PropertyRouteKind::Edge, "KNOWS", 1);
    assert!(
        enumerate_property_fragments(dir.path(), PropertyRouteKind::Edge, "_untyped")
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn append_edge_property_merge_adds_new_column() {
    let dir = TempDir::new().unwrap();
    let first = write_knows_edge_with_props(
        dir.path(),
        OntologyMode::Strict,
        std::collections::HashMap::from([("since".to_owned(), IrLiteral::Int(2020))]),
    );
    // Second session: a new KNOWS edge with an extra `weight` property.
    let second = write_knows_edge_with_props(
        dir.path(),
        OntologyMode::Strict,
        std::collections::HashMap::from([
            ("since".to_owned(), IrLiteral::Int(2021)),
            ("weight".to_owned(), IrLiteral::Float(0.5)),
        ]),
    );

    let rows = read_edge_property_rows(dir.path(), "KNOWS");
    assert_eq!(rows.len(), 2, "both edge rows retained");
    assert!(!rows[first.as_bytes()].values.contains_key("weight"));
    assert_eq!(
        rows[second.as_bytes()].values["weight"],
        IrLiteral::Float(0.5)
    );
    assert_canonical_fragments(dir.path(), PropertyRouteKind::Edge, "KNOWS", 2);
}

#[tokio::test]
async fn append_edge_property_merge_preserves_heterogeneous_scalar_types() {
    let dir = TempDir::new().unwrap();
    let first = write_knows_edge_with_props(
        dir.path(),
        OntologyMode::Strict,
        std::collections::HashMap::from([("x".to_owned(), IrLiteral::Int(1))]),
    );
    let second = write_knows_edge_with_props(
        dir.path(),
        OntologyMode::Strict,
        std::collections::HashMap::from([("x".to_owned(), IrLiteral::Str("two".to_owned()))]),
    );

    let rows = read_edge_property_rows(dir.path(), "KNOWS");
    assert_eq!(rows[first.as_bytes()].values["x"], IrLiteral::Int(1));
    assert_eq!(
        rows[second.as_bytes()].values["x"],
        IrLiteral::Str("two".into())
    );
}

#[tokio::test]
async fn append_edge_property_merge_null_first_across_flushes() {
    // An all-null edge-property column on disk must not pin the merged column to
    // Utf8 — a concrete value in the next flush fixes the type (mirror of the
    // node-property null-first test).
    let dir = TempDir::new().unwrap();
    let first = write_knows_edge_with_props(
        dir.path(),
        OntologyMode::Strict,
        std::collections::HashMap::from([("score".to_owned(), IrLiteral::Null)]),
    );
    let second = write_knows_edge_with_props(
        dir.path(),
        OntologyMode::Strict,
        std::collections::HashMap::from([("score".to_owned(), IrLiteral::Int(42))]),
    );

    let rows = read_edge_property_rows(dir.path(), "KNOWS");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[first.as_bytes()].values["score"], IrLiteral::Null);
    assert_eq!(rows[second.as_bytes()].values["score"], IrLiteral::Int(42));
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

    let knows = read_edge_property_rows(dir.path(), "KNOWS");
    assert_eq!(knows.len(), 1);
    assert!(knows.values().all(|row| row.values.contains_key("since")));
    assert!(knows.values().all(|row| !row.values.contains_key("rating")));
    let likes = read_edge_property_rows(dir.path(), "LIKES");
    assert_eq!(likes.len(), 1);
    assert_eq!(likes[edge.as_bytes()].values["rating"], IrLiteral::Int(5));
    assert_canonical_fragments(dir.path(), PropertyRouteKind::Edge, "KNOWS", 1);
    assert_canonical_fragments(dir.path(), PropertyRouteKind::Edge, "LIKES", 1);
}

#[tokio::test]
async fn edge_property_table_sql_and_direct_reads_merge_cross_fragment_mutations() {
    let dir = TempDir::new().unwrap();
    let (src, dst) = (new_v7(), new_v7());
    let (updated, retained, deleted) = (new_v7(), new_v7(), new_v7());

    let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    writer.create_node(src, TypeId(0)).unwrap();
    writer.create_node(dst, TypeId(0)).unwrap();
    for (edge, properties) in [
        (
            updated,
            HashMap::from([
                ("since".to_owned(), IrLiteral::Int(2020)),
                ("weight".to_owned(), IrLiteral::Int(7)),
            ]),
        ),
        (
            retained,
            HashMap::from([
                ("since".to_owned(), IrLiteral::Int(2019)),
                ("status".to_owned(), IrLiteral::Str("old".into())),
            ]),
        ),
        (
            deleted,
            HashMap::from([
                ("since".to_owned(), IrLiteral::Int(2018)),
                ("status".to_owned(), IrLiteral::Str("delete-me".into())),
            ]),
        ),
    ] {
        writer.create_edge(edge, "KNOWS", &src, &dst).unwrap();
        writer
            .set_edge_properties(&edge, Some("KNOWS"), properties)
            .unwrap();
    }
    writer.flush().unwrap();

    assert_eq!(
        set_edge_properties_rewrite(
            dir.path(),
            "KNOWS",
            &HashMap::from([
                (
                    *updated.as_bytes(),
                    HashMap::from([
                        ("since".to_owned(), IrLiteral::Int(2021)),
                        ("note".to_owned(), IrLiteral::Str("new".into())),
                    ]),
                ),
                (
                    *retained.as_bytes(),
                    HashMap::from([("status".to_owned(), IrLiteral::Str("current".into()),)]),
                ),
            ]),
        )
        .unwrap(),
        2
    );
    assert_eq!(
        remove_edge_properties(
            dir.path(),
            "KNOWS",
            &HashMap::from([(*updated.as_bytes(), HashSet::from(["weight".to_owned()]))]),
        )
        .unwrap(),
        1
    );
    assert_eq!(
        delete_edges(dir.path(), &HashSet::from([*deleted.as_bytes()])).unwrap(),
        1
    );
    assert_canonical_fragments(dir.path(), PropertyRouteKind::Edge, "KNOWS", 4);

    let direct = read_edge_property_rows(dir.path(), "KNOWS");
    assert_eq!(direct.len(), 2, "the deleted edge tombstone must win");
    assert_eq!(
        direct[updated.as_bytes()].values["since"],
        IrLiteral::Int(2021)
    );
    assert_eq!(
        direct[updated.as_bytes()].values["note"],
        IrLiteral::Str("new".into())
    );
    assert!(!direct[updated.as_bytes()].values.contains_key("weight"));
    assert_eq!(
        direct[retained.as_bytes()].values["status"],
        IrLiteral::Str("current".into())
    );

    let ctx = SessionContext::new();
    ctx.register_table(
        "edge_props",
        Arc::new(EdgePropertyTable::open_discovered(dir.path(), "KNOWS")),
    )
    .unwrap();
    let batches = ctx
        .sql("SELECT edge_uuid, since, status FROM edge_props ORDER BY edge_uuid LIMIT 10")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut sql = BTreeMap::new();
    for batch in batches {
        let uuids = batch
            .column_by_name("edge_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let since = batch
            .column_by_name("since")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let status = batch
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            let uuid: [u8; 16] = uuids.value(index).try_into().unwrap();
            assert!(
                sql.insert(
                    uuid,
                    (
                        since.value(index),
                        (!status.is_null(index)).then(|| status.value(index).to_owned()),
                    ),
                )
                .is_none(),
                "SQL overlay emitted more than one row for one edge UUID"
            );
        }
    }
    assert_eq!(sql.len(), direct.len());
    for (uuid, row) in &direct {
        let expected_since = match row.values["since"] {
            IrLiteral::Int(value) => value,
            ref value => panic!("unexpected since value: {value:?}"),
        };
        let expected_status = match row.values.get("status") {
            Some(IrLiteral::Str(value)) => Some(value.clone()),
            None => None,
            value => panic!("unexpected status value: {value:?}"),
        };
        assert_eq!(sql[uuid], (expected_since, expected_status));
    }

    let projected = ctx
        .sql("SELECT status FROM edge_props WHERE status IS NOT NULL LIMIT 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        projected
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    let status = projected[0]
        .column_by_name("status")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(status.value(0), "current");
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
    assert_canonical_fragments(dir.path(), PropertyRouteKind::Node, "_untyped", 1);
    assert_eq!(
        read_property_rows(dir.path(), "_untyped")[a.as_bytes()].values["name"],
        IrLiteral::Str("A".into())
    );
    assert_eq!(read_topology_generation(dir.path()).unwrap(), 1);

    // An empty flush stages nothing and must not bump either.
    w.flush().unwrap();
    assert_eq!(read_topology_generation(dir.path()).unwrap(), 1);
}
