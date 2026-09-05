//! Differential correctness corpus (#768, #1248, #1269): every fixed-hop query
//! runs through indexed and scan-build providers plus the independent legacy
//! relational lowering, asserting **byte-identical canonical rows**.
//!
//! Fixed-hop queries retain the same provider-backed `ExpandExec` plan in both
//! runs; only `adjacency=hit` versus `adjacency=building` changes. This stable
//! plan shape is what lets downstream `LIMIT` cancel traversal work.
//!
//! Sessions run in **Advisory** mode over typed edge files.

use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow::array::Array;
use tempfile::TempDir;

use graphforge_exec::{ExecutionResult, ExecutionSession};
use graphforge_ir::{Binder, GraphPlan, OntologyMode, RuntimeCatalog};
use graphforge_storage::GraphCatalog;
use graphforge_storage::adjacency::build_adjacency_index;

const TS: i64 = 1_700_000_000_000_000;

fn bind(query: &str, rc: &Arc<Mutex<RuntimeCatalog>>) -> GraphPlan {
    let binder = Binder::new(None, Arc::clone(rc), OntologyMode::Advisory);
    let ast = graphforge_cypher::parse(query).expect("parse");
    binder
        .bind(&ast)
        .unwrap_or_else(|e| panic!("bind {query:?}: {e:?}"))
}

fn session(dir: &Path, rc: &Arc<Mutex<RuntimeCatalog>>) -> ExecutionSession {
    let catalog = GraphCatalog::open(dir, None, &rc.lock().unwrap()).unwrap();
    ExecutionSession::new_with_target(catalog, None, dir.to_path_buf(), OntologyMode::Advisory)
        .unwrap()
}

/// Corpus fixture:
///
/// ```text
/// Alice(30) ─KNOWS{since:2020,weight:1.5}→ Bob(25) ─KNOWS{since:2021}→ Carol(35)
/// Alice     ─KNOWS (no props)──────────────→ Bob          Carol ─KNOWS→ Carol (self-loop)
/// Carol     ─OWNS─→ Dave(28)                Zed(99): isolated
/// Mallory: created with an edge, then DETACH DELETEd (sparse surrogate ids)
/// ```
async fn seed(dir: &Path) -> Arc<Mutex<RuntimeCatalog>> {
    let rc = Arc::new(Mutex::new(RuntimeCatalog::new()));
    for stmt in [
        "CREATE (:Person {name: 'Alice', age: 30})",
        "CREATE (:Person {name: 'Zed', age: 99})",
    ] {
        let plan = bind(stmt, &rc);
        session(dir, &rc).execute_create(&plan).await.unwrap();
    }
    for stmt in [
        "MATCH (a:Person {name: 'Alice'}) \
         CREATE (a)-[:KNOWS {since: 2020, weight: 1.5}]->(b:Person {name: 'Bob', age: 25})",
        "MATCH (b:Person {name: 'Bob'}) \
         CREATE (b)-[:KNOWS {since: 2021}]->(c:Person {name: 'Carol', age: 35})",
        // Parallel Alice→Bob edge WITHOUT properties (LEFT-join null parity).
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
        // Self-loop (undirected dedup case).
        "MATCH (c:Person {name: 'Carol'}) CREATE (c)-[:KNOWS]->(c)",
        // Second relation type.
        "MATCH (c:Person {name: 'Carol'}) \
         CREATE (c)-[:OWNS]->(d:Person {name: 'Dave', age: 28})",
        // Mallory exists briefly, then is DETACH DELETEd: surrogate id gap.
        "MATCH (a:Person {name: 'Alice'}) \
         CREATE (a)-[:KNOWS {since: 1999}]->(m:Person {name: 'Mallory'})",
        "MATCH (m:Person {name: 'Mallory'}) DETACH DELETE m",
    ] {
        let plan = bind(stmt, &rc);
        session(dir, &rc)
            .execute_write_statement(&plan)
            .await
            .unwrap_or_else(|e| panic!("seed {stmt:?}: {e}"));
    }
    rc
}

/// Canonical multiset of a result's rows: column-name-sorted `name=value`
/// rendering per row (NULL-normalized), rows sorted. The `rels` `List<Struct>`
/// column renders through Arrow's display, which covers its struct fields.
fn canonical_rows(result: &ExecutionResult) -> Vec<String> {
    let mut rows = Vec::new();
    for batch in &result.batches {
        let mut cols: Vec<(String, arrow::array::ArrayRef)> = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name().clone(), batch.column(i).clone()))
            .collect();
        cols.sort_by(|a, b| a.0.cmp(&b.0));
        for row in 0..batch.num_rows() {
            let mut parts = Vec::new();
            for (name, col) in &cols {
                let rendered = if col.is_null(row) {
                    "NULL".to_owned()
                } else {
                    // A render failure must FAIL the corpus, not normalize
                    // both runs to the same sentinel (false green).
                    arrow::util::display::array_value_to_string(col, row).unwrap_or_else(|e| {
                        panic!("failed to render column {name} at row {row}: {e}")
                    })
                };
                parts.push(format!("{name}={rendered}"));
            }
            rows.push(parts.join(","));
        }
    }
    rows.sort();
    rows
}

/// What the physical plans of the two runs must look like for this query.
#[derive(Clone, Copy, PartialEq)]
enum PlanShape {
    /// Fixed hops: the exact number of `ExpandExec` nodes must appear in both
    /// runs; only the adjacency source differs.
    FixedHopProvider { expand_execs: usize },
    /// Var-len: `VarLenExpandExec` both runs; the adjacency *source* differs
    /// (`adjacency=hit` vs `adjacency=building`).
    VarLen,
    /// Control: identical plan shape both runs (join/scan path everywhere).
    Control,
}

/// Run `query` with and without the index; assert identical canonical rows
/// and the expected plan-shape difference. Returns the rows.
async fn differential(
    dir: &Path,
    rc: &Arc<Mutex<RuntimeCatalog>>,
    query: &str,
    shape: PlanShape,
) -> Vec<String> {
    let bound_query = bind(query, rc);

    build_adjacency_index(dir, TS).unwrap();
    let indexed_session = session(dir, rc);
    let indexed_plan = indexed_session
        .explain_physical(&bound_query)
        .await
        .unwrap();
    let indexed = indexed_session.execute_plan(&bound_query).await.unwrap();

    std::fs::remove_dir_all(dir.join("indexes")).unwrap();
    let plain_session = session(dir, rc);
    let plain_plan = plain_session.explain_physical(&bound_query).await.unwrap();
    let plain = plain_session.execute_plan(&bound_query).await.unwrap();

    let relational = if matches!(shape, PlanShape::FixedHopProvider { .. }) {
        let reference = session(dir, rc).with_relational_fixed_hop_reference();
        let reference_plan = reference.explain_physical(&bound_query).await.unwrap();
        assert!(
            !reference_plan.contains("ExpandExec"),
            "{query}: relational oracle unexpectedly used provider expansion:\n{reference_plan}"
        );
        Some(reference.execute_plan(&bound_query).await.unwrap())
    } else {
        None
    };

    match shape {
        PlanShape::FixedHopProvider { expand_execs } => {
            assert_eq!(
                indexed_plan.matches("ExpandExec").count(),
                expand_execs,
                "{query}: indexed run has the wrong fixed-hop plan:\n{indexed_plan}"
            );
            assert_eq!(
                plain_plan.matches("ExpandExec").count(),
                expand_execs,
                "{query}: index-less run has the wrong fixed-hop plan:\n{plain_plan}"
            );
            assert!(
                indexed_plan.contains("ExpandExec") && indexed_plan.contains("adjacency=hit"),
                "{query}: indexed run should be adjacency-backed:\n{indexed_plan}"
            );
            assert!(
                plain_plan.contains("ExpandExec") && plain_plan.contains("adjacency=building"),
                "{query}: index-less run should scan-build through ExpandExec:\n{plain_plan}"
            );
        }
        PlanShape::VarLen => {
            assert!(
                indexed_plan.contains("VarLenExpandExec") && indexed_plan.contains("adjacency=hit"),
                "{query}: indexed var-len should hit:\n{indexed_plan}"
            );
            assert!(
                plain_plan.contains("VarLenExpandExec")
                    && plain_plan.contains("adjacency=building"),
                "{query}: index-less var-len should scan-build:\n{plain_plan}"
            );
        }
        PlanShape::Control => {}
    }

    let a = canonical_rows(&indexed);
    let b = canonical_rows(&plain);
    assert_eq!(a, b, "{query}: indexed vs scan-build rows differ");
    if let Some(relational) = relational {
        assert_eq!(
            a,
            canonical_rows(&relational),
            "{query}: provider vs relational rows differ"
        );
    }
    a
}

macro_rules! corpus_test {
    ($name:ident, $shape:expr, $query:expr, $expected_rows:expr) => {
        #[tokio::test]
        async fn $name() {
            let dir = TempDir::new().unwrap();
            let rc = seed(dir.path()).await;
            let rows = differential(dir.path(), &rc, $query, $shape).await;
            assert_eq!(rows.len(), $expected_rows, "{:?}", rows);
        }
    };
}

// ---------------------------------------------------------------------------
// Single-hop provider matrix (index hit vs scan-build)
// ---------------------------------------------------------------------------

// Alice→Bob (2020), Alice→Bob (no props), Bob→Carol, Carol→Carol.
corpus_test!(
    q01_single_hop_out_node_props,
    PlanShape::FixedHopProvider { expand_execs: 1 },
    "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS an, b.name AS bn",
    4
);
corpus_test!(
    q02_edge_prop_from_seeded_source,
    PlanShape::FixedHopProvider { expand_execs: 1 },
    "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person) RETURN r.since AS since",
    2
);
corpus_test!(
    q03_inline_edge_prop_filter,
    PlanShape::FixedHopProvider { expand_execs: 1 },
    "MATCH (a:Person)-[r:KNOWS {since: 2020}]->(b:Person) RETURN b.name AS bn",
    1
);
corpus_test!(
    q04_single_hop_in,
    PlanShape::FixedHopProvider { expand_execs: 1 },
    "MATCH (b:Person)<-[r:KNOWS]-(a:Person) RETURN a.name AS an, b.name AS bn",
    4
);
corpus_test!(
    q05_dst_filter_and_props,
    PlanShape::FixedHopProvider { expand_execs: 1 },
    "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE b.age > 28 RETURN b.name AS bn",
    2 // Bob→Carol(35) and the Carol self-loop's dst Carol(35)
);
corpus_test!(
    q06_chained_two_hop,
    PlanShape::FixedHopProvider { expand_execs: 2 },
    "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c.node_uuid",
    2 // two Alice→Bob edges × Bob→Carol
);
corpus_test!(
    q08_two_edge_props_and_dst_prop,
    PlanShape::FixedHopProvider { expand_execs: 1 },
    "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.weight AS w, b.name AS bn",
    4
);
corpus_test!(
    q19_second_rel_type_out,
    PlanShape::FixedHopProvider { expand_execs: 1 },
    "MATCH (c:Person)-[r:OWNS]->(d:Person) RETURN d.name AS dn",
    1
);

// ---------------------------------------------------------------------------
// Controls and special shapes
// ---------------------------------------------------------------------------

// Undirected single-hop with a QUALIFIED projection (#825): the join path used
// to lose `var_<n>` qualifiers after `union().distinct()` and error out; it now
// re-qualifies the union output (and drops self-loops from the In leg in lieu of
// Distinct), so both paths return byte-identical rows. From Carol: incoming
// Bob→Carol (b=Bob) + the Carol self-loop exactly once.
corpus_test!(
    q09_undirected_single_hop_qualified,
    PlanShape::FixedHopProvider { expand_execs: 1 },
    "MATCH (a:Person {name: 'Carol'})-[r:KNOWS]-(b) RETURN b.node_uuid",
    2 // Bob (incoming), Carol (self-loop, once)
);
// Undirected with a labelled dst + property projection (#825 error mode 2): the
// trailing `join_node_properties` over the re-qualified dst must resolve `b.name`
// unambiguously. Every Person is undirected-adjacent to its KNOWS neighbours.
corpus_test!(
    q23_undirected_labeled_dst_prop,
    PlanShape::FixedHopProvider { expand_execs: 1 },
    "MATCH (a:Person {name: 'Carol'})-[r:KNOWS]-(b:Person) RETURN b.name AS bn",
    2 // Bob (incoming), Carol (self-loop, once)
);
// Undirected with NO column projection (#825): the optimizer prunes the union to
// an empty schema; confirm it still executes (a zero-column union) and the
// self-loop is counted once. (The original q09 query, now on both paths.)
corpus_test!(
    q24_undirected_no_projection,
    PlanShape::FixedHopProvider { expand_execs: 1 },
    "MATCH (a:Person {name: 'Carol'})-[r:KNOWS]-(b) RETURN 1 AS one",
    2 // Bob (incoming), Carol (self-loop, once)
);
// Untyped single-hop in Advisory mode (#823): "*" now reads the UnionEdgeTable
// (every per-relation file), so both paths return one row per surviving edge —
// KNOWS ×4 (Alice→Bob ×2, Bob→Carol, Carol→Carol) + OWNS ×1 (Carol→Dave). The
// wildcard uses the same provider-backed node as typed traversal. (Was 0 rows
// pre-#823.)
corpus_test!(
    q10_wildcard_single_hop,
    PlanShape::FixedHopProvider { expand_execs: 1 },
    "MATCH (a:Person)-[r]->(b) RETURN 1 AS one",
    5
);
// Positive single-hop wildcard: from Carol, "*" reaches the KNOWS self-loop
// (Carol) AND the OWNS target (Dave) — proves the untyped scan spans relation
// types, not just KNOWS (the pre-#823 bug returned nothing here).
corpus_test!(
    q22_single_hop_wildcard_out,
    PlanShape::FixedHopProvider { expand_execs: 1 },
    "MATCH (c:Person {name: 'Carol'})-[r]->(x:Person) RETURN x.name AS xn",
    2 // Carol (self-loop KNOWS), Dave (OWNS)
);

// ---------------------------------------------------------------------------
// Var-len (VarLenExpandExec both runs; adjacency source differs)
// ---------------------------------------------------------------------------

corpus_test!(
    q11_var_len_bounded,
    PlanShape::VarLen,
    "MATCH (a:Person {name: 'Alice'})-[r:KNOWS*1..2]->(b:Person) RETURN DISTINCT b.node_uuid",
    2 // Bob, Carol
);
corpus_test!(
    q12_var_len_unbounded,
    PlanShape::VarLen,
    "MATCH (a:Person {name: 'Alice'})-[r:KNOWS*]->(b:Person) RETURN DISTINCT b.node_uuid",
    2
);
corpus_test!(
    q13_var_len_zero_hop,
    PlanShape::VarLen,
    "MATCH (a:Person {name: 'Alice'})-[r:KNOWS*0..2]->(b:Person) RETURN DISTINCT b.node_uuid",
    3 // Alice (0-hop), Bob, Carol
);
corpus_test!(
    q14_var_len_rels_list,
    PlanShape::VarLen,
    "MATCH (a:Person {name: 'Alice'})-[r:KNOWS*1..2]->(b:Person) RETURN r AS rels",
    4 // 1-hop: a→b ×2 (parallel edges); 2-hop: each × b→c = 2 more
);
corpus_test!(
    q16_var_len_in,
    PlanShape::VarLen,
    "MATCH (c:Person {name: 'Carol'})<-[r:KNOWS*1..2]-(x:Person) RETURN DISTINCT x.node_uuid",
    3 // Bob (1 hop), Alice (2 hops), Carol (self-loop 1 hop)
);
corpus_test!(
    q17_var_len_exact_hops,
    PlanShape::VarLen,
    "MATCH (a:Person {name: 'Alice'})-[r:KNOWS*2..2]->(c:Person) RETURN c.node_uuid",
    2 // two parallel first hops × Bob→Carol
);

// Untyped var-len in Advisory mode (#823): "*" unions KNOWS ∪ OWNS. Both runs
// must agree — indexed via the `_all` CSR Hit, index-less via the scan-build
// union over `read_edges("*")`. Dave is reachable from Bob ONLY through the
// second-hop OWNS edge (Bob→Carol→Dave), so a row for Dave proves the union
// includes the non-KNOWS relation (the pre-#823 bug returned empty here).
corpus_test!(
    q20_var_len_wildcard_out,
    PlanShape::VarLen,
    "MATCH (b:Person {name: 'Bob'})-[r*1..2]->(x:Person) RETURN DISTINCT x.node_uuid",
    2 // Carol (1-hop KNOWS), Dave (2-hop via OWNS)
);
// Same wildcard expansion returning the relationship list — exercises the
// edge-records path (`build_edge_records`/`build_edge_list_column`) over the
// union, the hazard the plan flags: adjacency-universe and edge-records-universe
// must match or this errors with "no record for edge_id".
corpus_test!(
    q21_var_len_wildcard_rels_list,
    PlanShape::VarLen,
    "MATCH (b:Person {name: 'Bob'})-[r*1..2]->(x:Person) RETURN r AS rels",
    3 // [Bob→Carol]; [Bob→Carol, Carol→Carol]; [Bob→Carol, Carol→Dave(OWNS)]
);

// ---------------------------------------------------------------------------
// OPTIONAL MATCH wrapper + invalidation pairing
// ---------------------------------------------------------------------------

corpus_test!(
    q07_optional_match,
    PlanShape::Control,
    "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m) RETURN n.node_uuid, m.node_uuid",
    6 // Alice×2 (parallel edges), Bob×1, Carol×1 (self-loop), Dave NULL, Zed NULL
);

#[tokio::test]
async fn q18_same_session_write_invalidates_indexed_run() {
    // Index built; one session reads, writes, reads — the post-write read must
    // match a no-index run over the post-write graph (provider invalidation).
    let dir = TempDir::new().unwrap();
    let rc = seed(dir.path()).await;
    build_adjacency_index(dir.path(), TS).unwrap();

    let query = "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN b.name AS bn";
    let plan = bind(query, &rc);
    let s = session(dir.path(), &rc);
    let before = s.execute_plan(&plan).await.unwrap();
    assert_eq!(canonical_rows(&before).len(), 4);

    let extend = bind(
        "MATCH (c:Person {name: 'Carol'}) CREATE (c)-[:KNOWS {since: 2024}]->(z:Person {name: 'Zoe'})",
        &rc,
    );
    s.execute_write_statement(&extend).await.unwrap();
    let after_indexed = canonical_rows(&s.execute_plan(&plan).await.unwrap());

    std::fs::remove_dir_all(dir.path().join("indexes")).unwrap();
    let after_plain = canonical_rows(&session(dir.path(), &rc).execute_plan(&plan).await.unwrap());
    assert_eq!(after_indexed, after_plain);
    assert!(after_indexed.iter().any(|r| r.contains("bn=Zoe")));
}
