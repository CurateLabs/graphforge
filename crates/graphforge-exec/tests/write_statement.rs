//! Mixed-write statement driver tests (#792 Step 2): parse → bind →
//! `execute_write_statement`, exercising clause-ordered sequencing, the
//! pending-buffer interactions, and the single staged commit — directly
//! against the session (the graphforge-api router swap is the next slice).

use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow::array::{Int64Array, UInt64Array};
use tempfile::TempDir;

use graphforge_core::GfError;
use graphforge_exec::{ExecutionResult, ExecutionSession, MutationKind, MutationSubjectKind};
use graphforge_ir::{Binder, GraphPlan, IrLiteral, OntologyMode, RuntimeCatalog};
use graphforge_storage::{
    GraphCatalog, PropertyOverlayLimits, PropertyRouteKind, enumerate_property_fragments,
    visit_authenticated_property_snapshots,
};

/// Bind `query` in Exploratory mode against the shared runtime catalog.
fn bind(query: &str, rt: &Arc<Mutex<RuntimeCatalog>>) -> GraphPlan {
    let binder = Binder::new(None, Arc::clone(rt), OntologyMode::Exploratory);
    let ast = graphforge_cypher::parse(query).expect("parse");
    binder
        .bind(&ast)
        .unwrap_or_else(|e| panic!("bind {query:?}: {e:?}"))
}

/// A session over `dir` with a freshly-opened catalog snapshot.
fn session(dir: &Path, rt: &Arc<Mutex<RuntimeCatalog>>) -> ExecutionSession {
    let catalog = GraphCatalog::open(dir, None, &rt.lock().unwrap()).expect("open catalog");
    ExecutionSession::new_with_target(catalog, None, dir.to_path_buf(), OntologyMode::Exploratory)
        .expect("session")
}

/// Seed the graph by running single-write CREATE statements the legacy way.
async fn seed(dir: &Path, rt: &Arc<Mutex<RuntimeCatalog>>, queries: &[&str]) {
    for q in queries {
        let plan = bind(q, rt);
        session(dir, rt)
            .execute_create(&plan)
            .await
            .unwrap_or_else(|e| panic!("seed {q:?}: {e}"));
    }
}

/// Run a mixed statement through the driver on a fresh catalog snapshot.
async fn run(
    dir: &Path,
    rt: &Arc<Mutex<RuntimeCatalog>>,
    stmt: &str,
) -> Result<ExecutionResult, GfError> {
    let plan = bind(stmt, rt);
    session(dir, rt).execute_write_statement(&plan).await
}

/// The six counters from a driver summary.
fn counters(result: &ExecutionResult) -> [u64; 6] {
    let batch = &result.batches[0];
    core::array::from_fn(|i| {
        batch
            .column(i)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0)
    })
}

/// Total rows of a (possibly absent) parquet file.
fn rows(path: &Path) -> usize {
    if !path.exists() {
        return 0;
    }
    let file = std::fs::File::open(path).unwrap();
    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .sum()
}

fn logical_property_rows(dir: &Path, route: &str) -> Vec<graphforge_storage::PropertySnapshotRow> {
    let scratch = TempDir::new().unwrap();
    let mut rows = Vec::new();
    visit_authenticated_property_snapshots(
        dir,
        PropertyRouteKind::Node,
        route,
        scratch.path(),
        PropertyOverlayLimits::default(),
        |row| {
            assert!(!row.tombstone);
            rows.push(row);
            Ok(())
        },
    )
    .unwrap();
    rows
}

#[tokio::test]
async fn create_then_delete_nets_zero_on_disk_counts_both() {
    let dir = TempDir::new().unwrap();
    let rt = Arc::new(Mutex::new(RuntimeCatalog::new()));

    let r = run(dir.path(), &rt, "CREATE (n:P) DELETE n").await.unwrap();
    assert_eq!(counters(&r), [1, 0, 1, 0, 0, 0], "created AND deleted");
    let receipt = r.mutation_receipt.as_ref().expect("write receipt");
    assert_eq!(
        receipt
            .effects
            .iter()
            .map(|effect| effect.kind)
            .collect::<Vec<_>>(),
        [MutationKind::CreateNode, MutationKind::Delete]
    );
    assert_eq!(receipt.effects[0].outputs.len(), 1);
    assert_eq!(
        receipt.effects[0].outputs[0].kind,
        MutationSubjectKind::Node
    );
    assert_eq!(receipt.effects[0].outputs[0], receipt.effects[1].inputs[0]);
    assert_eq!(rows(&dir.path().join("topology/nodes.parquet")), 0);
}

#[tokio::test]
async fn create_edge_then_plain_delete_errors_with_nothing_persisted() {
    // The #792 scenario: the edge created IN-STATEMENT makes the plain DELETE
    // illegal — and the abort must leave the graph byte-identical (the staged
    // commit never ran).
    let dir = TempDir::new().unwrap();
    let rt = Arc::new(Mutex::new(RuntimeCatalog::new()));
    seed(dir.path(), &rt, &["CREATE (:P {name:'a'})"]).await;
    assert_eq!(
        graphforge_storage::read_nodes(dir.path())
            .unwrap()
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );

    let err = run(
        dir.path(),
        &rt,
        "MATCH (a:P) CREATE (a)-[:R]->(b:T) DELETE a",
    )
    .await
    .expect_err("plain DELETE with an in-statement edge must error");
    assert!(
        err.to_string().contains("still has relationships"),
        "got {err}"
    );
    // Nothing persisted: no T node, no edge file, a intact.
    assert_eq!(rows(&dir.path().join("topology/nodes.parquet")), 1);
    assert_eq!(
        rows(&dir.path().join("topology/edges/_exploratory.parquet")),
        0
    );
}

#[tokio::test]
async fn create_edge_then_detach_delete_drops_pending_edge() {
    let dir = TempDir::new().unwrap();
    let rt = Arc::new(Mutex::new(RuntimeCatalog::new()));
    seed(dir.path(), &rt, &["CREATE (:P {name:'a'})"]).await;

    let r = run(
        dir.path(),
        &rt,
        "MATCH (a:P) CREATE (a)-[:R]->(b:T) DETACH DELETE a",
    )
    .await
    .unwrap();
    // a deleted (committed), b created, the pending edge cancelled — created
    // and deleted both count; T is a newly introduced label token.
    assert_eq!(counters(&r), [1, 1, 1, 1, 0, 1]);
    // b survives alone; the edge never hit disk.
    assert_eq!(
        graphforge_storage::read_nodes(dir.path())
            .unwrap()
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    assert_eq!(
        rows(&dir.path().join("topology/edges/_exploratory.parquet")),
        0
    );
}

#[tokio::test]
async fn set_on_created_node_lands_in_buffer() {
    let dir = TempDir::new().unwrap();
    let rt = Arc::new(Mutex::new(RuntimeCatalog::new()));

    let r = run(dir.path(), &rt, "CREATE (n:P) SET n.age = 41")
        .await
        .unwrap();
    assert_eq!(counters(&r), [1, 0, 0, 0, 1, 0]);
    assert_eq!(rows(&dir.path().join("topology/nodes.parquet")), 1);
    // The property merged into the buffered row and became authenticated
    // immutable logical authority.
    let fragments =
        enumerate_property_fragments(dir.path(), PropertyRouteKind::Node, "_untyped").unwrap();
    assert_eq!(fragments.len(), 1);
    assert_ne!(fragments[0].id.generation, 0);
    assert_eq!(
        fragments[0].path.file_name().unwrap().to_str().unwrap(),
        fragments[0].id.file_name()
    );
    let properties = logical_property_rows(dir.path(), "_untyped");
    assert_eq!(properties.len(), 1);
    assert_eq!(properties[0].values["age"], IrLiteral::Int(41));
}

#[tokio::test]
async fn remove_absent_property_is_an_explicit_noop_receipt() {
    let dir = TempDir::new().unwrap();
    let rt = Arc::new(Mutex::new(RuntimeCatalog::new()));
    seed(dir.path(), &rt, &["CREATE (:P {name:'a'})"]).await;

    let result = run(dir.path(), &rt, "MATCH (n:P) REMOVE n.missing")
        .await
        .unwrap();

    assert_eq!(counters(&result), [0, 0, 0, 0, 0, 0]);
    assert!(
        result
            .mutation_receipt
            .as_ref()
            .is_some_and(graphforge_exec::MutationReceipt::is_empty)
    );
}

#[tokio::test]
async fn set_after_delete_errors() {
    let dir = TempDir::new().unwrap();
    let rt = Arc::new(Mutex::new(RuntimeCatalog::new()));
    seed(dir.path(), &rt, &["CREATE (:P {name:'a'})"]).await;

    let err = run(dir.path(), &rt, "MATCH (a:P) DELETE a SET a.x = 1")
        .await
        .expect_err("writing to a deleted entity must error");
    assert!(
        err.to_string().contains("deleted in this statement"),
        "got {err}"
    );
    // Abort pre-commit: a survives.
    assert_eq!(rows(&dir.path().join("topology/nodes.parquet")), 1);
}

#[tokio::test]
async fn two_set_clauses_apply_in_order() {
    // Two Set ops passed the Step-1 guard but died in lowering before the
    // driver existed — now they sequence.
    let dir = TempDir::new().unwrap();
    let rt = Arc::new(Mutex::new(RuntimeCatalog::new()));
    seed(dir.path(), &rt, &["CREATE (:P {name:'a'})"]).await;

    let r = run(dir.path(), &rt, "MATCH (a:P) SET a.x = 1 SET a.y = 2")
        .await
        .unwrap();
    assert_eq!(counters(&r), [0, 0, 0, 0, 2, 0]);
}

#[tokio::test]
async fn later_set_reads_statement_local_property_value() {
    let dir = TempDir::new().unwrap();
    let rt = Arc::new(Mutex::new(RuntimeCatalog::new()));
    seed(dir.path(), &rt, &["CREATE (:P {name:'a'})"]).await;

    let result = run(dir.path(), &rt, "MATCH (a:P) SET a.x = 1 SET a.y = a.x + 1")
        .await
        .expect("later SET reads the statement-local overlay");
    assert_eq!(counters(&result), [0, 0, 0, 0, 2, 0]);
}

#[tokio::test]
async fn second_create_references_first_with_distinct_surrogates() {
    // Shared-writer fix: two CREATE clauses used to mint colliding node_ids
    // via two writers seeded from the same on-disk maximum.
    let dir = TempDir::new().unwrap();
    let rt = Arc::new(Mutex::new(RuntimeCatalog::new()));

    let r = run(dir.path(), &rt, "CREATE (a:P) CREATE (a)-[:K]->(b:P)")
        .await
        .unwrap();
    assert_eq!(counters(&r), [2, 1, 0, 0, 0, 0]);
    assert_eq!(rows(&dir.path().join("topology/nodes.parquet")), 2);
    assert_eq!(
        rows(&dir.path().join("topology/edges/_exploratory.parquet")),
        1
    );

    // Distinct node_id surrogates.
    let file = std::fs::File::open(dir.path().join("topology/nodes.parquet")).unwrap();
    let mut ids = Vec::new();
    for batch in parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap()
    {
        let batch = batch.unwrap();
        let col = batch
            .column_by_name("node_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .clone();
        ids.extend(col.values().iter().copied());
    }
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 2, "surrogates must be distinct");
}

#[tokio::test]
async fn terminal_return_after_write_phases_projects_frontier() {
    let dir = TempDir::new().unwrap();
    let rt = Arc::new(Mutex::new(RuntimeCatalog::new()));

    let r = run(dir.path(), &rt, "CREATE (n:P) DELETE n RETURN 1 AS ok")
        .await
        .expect("terminal RETURN after writes should project rows");
    assert_eq!(r.stats.rows_produced, 1);
    let ok = r.batches[0]
        .column_by_name("ok")
        .expect("ok column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 ok")
        .value(0);
    assert_eq!(ok, 1);
    let side_effects = r.side_effects.expect("write side effects");
    assert_eq!(side_effects.nodes_created, 1);
    assert_eq!(side_effects.nodes_deleted, 1);
    assert_eq!(rows(&dir.path().join("topology/nodes.parquet")), 0);
}

#[tokio::test]
async fn graph_read_after_write_sees_pending_nodes() {
    let dir = TempDir::new().unwrap();
    let rt = Arc::new(Mutex::new(RuntimeCatalog::new()));

    let result = run(dir.path(), &rt, "CREATE (n:P) WITH n MATCH (m:P) RETURN m")
        .await
        .expect("MATCH after a write should see the statement-local node");
    assert_eq!(result.stats.rows_produced, 1);
    assert_eq!(result.side_effects.unwrap().nodes_created, 1);
    assert_eq!(rows(&dir.path().join("topology/nodes.parquet")), 1);
}

#[tokio::test]
async fn graph_read_after_write_rolls_back_after_later_failure() {
    let dir = TempDir::new().unwrap();
    let rt = Arc::new(Mutex::new(RuntimeCatalog::new()));

    let err = run(
        dir.path(),
        &rt,
        "CREATE (n:P) WITH n MATCH (m:P) DELETE m SET m.x = 1",
    )
    .await
    .expect_err("SET after deleting the pending MATCH result must fail");
    assert!(err.to_string().contains("deleted"), "got {err}");
    assert_eq!(
        rows(&dir.path().join("topology/nodes.parquet")),
        0,
        "a failure after the pending read must roll back the whole statement"
    );
}

// ---------------------------------------------------------------------------
// topology_generation bumping (#759)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn statement_commit_bumps_topology_generation_only_for_topology() {
    let dir = TempDir::new().unwrap();
    let rt = Arc::new(Mutex::new(RuntimeCatalog::new()));
    seed(dir.path(), &rt, &["CREATE (:Person {name: 'A'})"]).await;
    assert_eq!(
        graphforge_storage::read_topology_generation(dir.path()).unwrap(),
        1,
        "the seeding CREATE is one committed topology batch"
    );

    // Node + edge CREATE through the driver: both commit as one batch —
    // exactly one bump.
    run(
        dir.path(),
        &rt,
        "MATCH (a:Person {name: 'A'}) CREATE (a)-[:KNOWS]->(b:Person {name: 'B'})",
    )
    .await
    .unwrap();
    assert_eq!(
        graphforge_storage::read_topology_generation(dir.path()).unwrap(),
        2
    );

    // SET-only statement: the net batch stages only property files — no bump.
    run(dir.path(), &rt, "MATCH (n:Person) SET n.age = 1")
        .await
        .unwrap();
    assert_eq!(
        graphforge_storage::read_topology_generation(dir.path()).unwrap(),
        2
    );

    // DETACH DELETE: node + incident edges + property rewrites commit as one
    // batch — exactly one bump.
    run(
        dir.path(),
        &rt,
        "MATCH (n:Person {name: 'A'}) DETACH DELETE n",
    )
    .await
    .unwrap();
    assert_eq!(
        graphforge_storage::read_topology_generation(dir.path()).unwrap(),
        3
    );
}

#[tokio::test]
async fn mutation_plans_relocate_and_bind_only_selected_write_resources() {
    use datafusion::logical_expr::LogicalPlan;
    use std::hash::{Hash, Hasher};
    for (query, expected_left, expected_right) in [
        (
            "CREATE (:Person {name:'new'})",
            vec!["left", "new"],
            vec!["new", "right"],
        ),
        (
            "MATCH (n:Person) SET n.name = n.name + '-set'",
            vec!["left-set"],
            vec!["right-set"],
        ),
        ("MATCH (n:Person) REMOVE n.name", vec![], vec![]),
        ("MATCH (n:Person) DELETE n", vec![], vec![]),
    ] {
        let left = TempDir::new().unwrap();
        let right = TempDir::new().unwrap();
        let rt = Arc::new(Mutex::new(RuntimeCatalog::new()));
        seed(left.path(), &rt, &["CREATE (:Person {name:'left'})"]).await;
        seed(right.path(), &rt, &["CREATE (:Person {name:'right'})"]).await;
        let ir = bind(query, &rt);
        let lower = |root: &Path| {
            let catalog = GraphCatalog::open(root, None, &rt.lock().unwrap()).unwrap();
            graphforge_rel::GraphPlanLowerer::new_for_writes(
                &graphforge_storage::lowering_snapshot(Some(&catalog), Some(root)).unwrap(),
                None,
                OntologyMode::Exploratory,
            )
            .unwrap()
            .lower_plan(&ir)
            .unwrap()
        };
        let plan = lower(left.path());
        let relocated = lower(right.path());
        assert_eq!(plan, relocated, "{query}");
        let hash = |plan: &LogicalPlan| {
            let mut hash = std::collections::hash_map::DefaultHasher::new();
            plan.hash(&mut hash);
            hash.finish()
        };
        assert_eq!(hash(&plan), hash(&relocated));
        let explain = plan.display_indent().to_string();
        assert_eq!(explain, relocated.display_indent().to_string());
        assert!(!explain.contains(left.path().to_str().unwrap()));
        assert!(!explain.contains(right.path().to_str().unwrap()));
        let left_session = session(left.path(), &rt);
        let right_session = session(right.path(), &rt);
        let left_state = left_session.context().state();
        let right_state = right_session.context().state();
        let (left_physical, right_physical) = tokio::join!(
            left_state.create_physical_plan(&plan),
            right_state.create_physical_plan(&plan),
        );
        let left_stream = datafusion::physical_plan::execute_stream(
            left_physical.unwrap(),
            left_session.context().task_ctx(),
        )
        .unwrap();
        let right_stream = datafusion::physical_plan::execute_stream(
            right_physical.unwrap(),
            right_session.context().task_ctx(),
        )
        .unwrap();
        drop(left_session);
        drop(right_session);
        let left_result = datafusion::physical_plan::common::collect(left_stream)
            .await
            .unwrap();
        assert_eq!(left_result.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        let names = |root: &Path| {
            let mut names: Vec<_> = logical_property_rows(root, "_untyped")
                .into_iter()
                .filter_map(|row| match row.values.get("name") {
                    Some(graphforge_ir::IrLiteral::Str(name)) => Some(name.clone()),
                    _ => None,
                })
                .collect();
            names.sort();
            names
        };
        assert_eq!(
            names(right.path()),
            ["right"],
            "the other retained stream has not been polled"
        );
        let right_result = datafusion::physical_plan::common::collect(right_stream)
            .await
            .unwrap();
        assert_eq!(right_result.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        assert_eq!(names(left.path()), expected_left, "{query}");
        assert_eq!(names(right.path()), expected_right, "{query}");
        assert_eq!(
            plan, relocated,
            "binding leaves logical descriptors untouched"
        );
    }
}

#[tokio::test]
async fn statement_and_literal_writes_require_explicit_writable_context() {
    let dir = TempDir::new().unwrap();
    let rt = Arc::new(Mutex::new(RuntimeCatalog::new()));
    seed(dir.path(), &rt, &["CREATE (:Person {name:'original'})"]).await;
    for query in [
        "CREATE (:Person {name:'new'})",
        "MATCH (n:Person) SET n.name = 'changed'",
        "MATCH (n:Person) REMOVE n.name",
        "MATCH (n:Person) DELETE n",
        "MERGE (:Person {name:'original'})",
    ] {
        let plan = bind(query, &rt);
        let readonly = session(dir.path(), &rt).restrict_to_reads();
        let catalog = GraphCatalog::open(dir.path(), None, &rt.lock().unwrap()).unwrap();
        let missing = ExecutionSession::new(catalog, None).unwrap();
        for (session, code) in [
            (readonly, "GF_WRITE_RESOURCE_READ_ONLY"),
            (missing, "GF_WRITE_RESOURCE_MISSING"),
        ] {
            let error = session.execute_write_statement(&plan).await.unwrap_err();
            assert!(error.to_string().contains(code), "{query}: {error}");
            let error = session.execute_create(&plan).await.unwrap_err();
            assert!(error.to_string().contains(code), "{query}: {error}");
        }
    }
    let properties = logical_property_rows(dir.path(), "_untyped");
    assert_eq!(properties.len(), 1);
    assert_eq!(
        properties[0].values.get("name"),
        Some(&graphforge_ir::IrLiteral::Str("original".into()))
    );
}

#[tokio::test]
async fn mixed_create_set_merge_preserves_buffered_visibility_and_atomic_rollback() {
    for fail in [false, true] {
        let dir = TempDir::new().unwrap();
        let rt = Arc::new(Mutex::new(RuntimeCatalog::new()));
        let query = if fail {
            "CREATE (n:P {x:1}) SET n.x=2 MERGE (m:P {x:2}) DELETE m SET m.x=3"
        } else {
            "CREATE (n:P {x:1}) SET n.x=2 MERGE (m:P {x:2}) RETURN m"
        };
        let result = run(dir.path(), &rt, query).await;
        if fail {
            assert!(result.unwrap_err().to_string().contains("deleted"));
            assert_eq!(rows(&dir.path().join("topology/nodes.parquet")), 0);
            assert!(logical_property_rows(dir.path(), "_untyped").is_empty());
        } else {
            let result = result.unwrap();
            assert_eq!(result.stats.rows_produced, 1);
            assert_eq!(result.side_effects.as_ref().unwrap().nodes_created, 1);
            let receipt = result.mutation_receipt.unwrap();
            assert_eq!(
                receipt
                    .effects
                    .iter()
                    .filter(|effect| effect.kind == MutationKind::CreateNode)
                    .count(),
                1
            );
            assert_eq!(rows(&dir.path().join("topology/nodes.parquet")), 1);
            let properties = logical_property_rows(dir.path(), "_untyped");
            assert_eq!(properties.len(), 1);
            assert_eq!(
                properties[0].values.get("x"),
                Some(&graphforge_ir::IrLiteral::Int(2))
            );
        }
    }
}
