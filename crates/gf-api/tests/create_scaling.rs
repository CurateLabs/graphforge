//! Deterministic dependent-CREATE scaling and atomicity gates (#1264).

use std::sync::Mutex;

use arrow::array::Int64Array;
use gf_api::GraphForge;
use gf_storage::IoSnapshot;

/// Serializes the process-global storage counters used by the assertions.
static IO_STATS_LOCK: Mutex<()> = Mutex::new(());

fn dependent_create_query(clauses: usize) -> String {
    let mut query = "CREATE (root:Root {name: 'root'})".to_owned();
    for index in 0..clauses {
        query.push_str(&format!(
            " CREATE (root)-[:HAS]->(n{index}:Leaf {{value: {index}}})"
        ));
    }
    query
}

fn scalar_count(forge: &GraphForge, query: &str) -> i64 {
    let result = forge.execute(query).expect("count query executes");
    result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count is Int64")
        .value(0)
}

fn measure(clauses: usize) -> IoSnapshot {
    let forge = GraphForge::new(None).expect("in-memory forge");
    gf_storage::io_stats::reset();
    let result = forge
        .execute(&dependent_create_query(clauses))
        .expect("dependent CREATE executes");
    let io = gf_storage::io_stats::snapshot();
    let effects = result.side_effects.expect("write side effects");
    assert_eq!(effects.nodes_created, u64::try_from(clauses + 1).unwrap());
    assert_eq!(
        effects.relationships_created,
        u64::try_from(clauses).unwrap()
    );
    assert_eq!(effects.properties_set, u64::try_from(clauses + 1).unwrap());
    assert_eq!(effects.labels_added, 2);
    assert_eq!(
        scalar_count(&forge, "MATCH (n) RETURN count(*)"),
        i64::try_from(clauses + 1).unwrap()
    );
    assert_eq!(
        scalar_count(&forge, "MATCH ()-[r]->() RETURN count(*)"),
        i64::try_from(clauses).unwrap()
    );
    io
}

#[test]
fn dependent_create_storage_work_stays_bounded_at_ten_x() {
    let _guard = IO_STATS_LOCK.lock().expect("I/O stats lock");
    let small = measure(25);
    let large = measure(250);
    let small_full_reads = small.node_full_reads + small.edge_full_reads;
    let large_full_reads = large.node_full_reads + large.edge_full_reads;

    assert!(small.rewrite_commits > 0, "{small:?}");
    assert!(
        large.rewrite_commits <= small.rewrite_commits * 3,
        "{small:?} {large:?}"
    );
    assert!(small_full_reads > 0, "{small:?}");
    assert!(
        large_full_reads <= small_full_reads * 3,
        "{small:?} {large:?}"
    );
}

#[test]
fn late_create_failure_persists_no_partial_graph() {
    let _guard = IO_STATS_LOCK.lock().expect("I/O stats lock");
    let forge = GraphForge::new(None).expect("in-memory forge");
    let mut query = dependent_create_query(25);
    query.push_str(" CREATE (bad:Leaf {node_uuid: 'reserved'})");

    let error = forge.execute(&query).expect_err("reserved field must fail");
    assert!(error.to_string().contains("reserved node topology field"));
    assert_eq!(scalar_count(&forge, "MATCH (n) RETURN count(*)"), 0);
    assert_eq!(scalar_count(&forge, "MATCH ()-[r]->() RETURN count(*)"), 0);
}

#[test]
fn later_create_property_expression_keeps_its_input_binding() {
    let _guard = IO_STATS_LOCK.lock().expect("I/O stats lock");
    let forge = GraphForge::new(None).expect("in-memory forge");
    forge
        .execute("CREATE (a:Node {value: 7}) CREATE (:Node {value: a.value})")
        .expect("dependent property CREATE executes");
    assert_eq!(
        scalar_count(&forge, "MATCH (n:Node) WHERE n.value = 7 RETURN count(*)",),
        2
    );
}
