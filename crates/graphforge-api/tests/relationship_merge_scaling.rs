//! Relationship MERGE candidate work stays bounded by topology plus endpoint matches.

use graphforge_api::GraphForge;
use graphforge_storage::io_stats;
use tempfile::TempDir;

fn measured_merge(rows: usize, wide: bool) -> (i64, io_stats::IoSnapshot) {
    let dir = TempDir::new().expect("temporary project directory");
    let forge = GraphForge::new(dir.path().to_str()).expect("persistent GraphForge");

    let properties = if wide {
        (0..48)
            .map(|index| format!("p{index}:{index}"))
            .collect::<Vec<_>>()
            .join(",")
    } else {
        "p:1".to_owned()
    };
    let mut patterns = vec!["(a:A {id:1})".to_owned(), "(b:B {id:2})".to_owned()];
    for _ in 0..4 {
        patterns.push(format!("(a)-[:REL {{{properties}}}]->(b)"));
    }
    // Fill the same topology and property route with unrelated endpoint pairs.
    for index in 0..16 {
        patterns.push(format!(
            "(:Other {{id:{index}}})-[:REL {{{properties}}}]->(:Other {{id:{}}})",
            index + 100
        ));
    }
    forge
        .execute(&format!("CREATE {}", patterns.join(", ")))
        .expect("seed matching and unrelated parallel edges");

    let _capture = io_stats::CaptureScope::install();
    io_stats::reset();
    let result = forge
        .execute(&format!(
            "UNWIND range(1, {rows}) AS i \
             MATCH (a:A {{id:1}}), (b:B {{id:2}}) \
             MERGE (a)-[r:REL]->(b) RETURN count(r) AS count"
        ))
        .expect("match existing parallel edges");
    let count = result.batches[0]
        .column_by_name("count")
        .expect("count column")
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("integer count")
        .value(0);
    let work = io_stats::snapshot().expect("captured relationship MERGE counters");
    (count, work)
}

#[test]
fn relationship_merge_batches_property_reads_and_indexes_endpoint_candidates() {
    for wide in [false, true] {
        let (one_count, one) = measured_merge(1, wide);
        let (many_count, many) = measured_merge(32, wide);

        assert_eq!(one_count, 4);
        assert_eq!(many_count, 128);
        assert_eq!(one.relationship_merge_topology_rows, 20, "{one:#?}");
        assert_eq!(many.relationship_merge_topology_rows, 20, "{many:#?}");
        assert_eq!(one.relationship_merge_candidate_rows, 4, "{one:#?}");
        assert_eq!(many.relationship_merge_candidate_rows, 128, "{many:#?}");
        assert_eq!(
            one.relationship_merge_property_rows,
            many.relationship_merge_property_rows
        );
        assert!(one.relationship_merge_property_rows > 0, "{one:#?}");
        assert_eq!(
            one.edge_full_reads, many.edge_full_reads,
            "{one:#?} / {many:#?}"
        );
        assert_eq!(
            one.edge_full_rows, many.edge_full_rows,
            "{one:#?} / {many:#?}"
        );
        assert!(
            many.relationship_merge_candidate_rows < many.relationship_merge_topology_rows * 32,
            "endpoint index should avoid rescanning unrelated topology rows: {many:#?}"
        );
    }
}

#[test]
fn relationship_merge_keeps_undirected_parallel_matches_and_null_rejection() {
    let forge = GraphForge::new(None).expect("in-memory GraphForge");
    forge
        .execute(
            "CREATE (a:A {id:1}), (b:B {id:2}) \
             CREATE (a)-[:REL {p:1}]->(b), (a)-[:REL {p:1}]->(b), \
                    (b)-[:REL {p:1}]->(a)",
        )
        .expect("seed directed and reverse parallel edges");

    let undirected = forge
        .execute(
            "MATCH (a:A {id:1}), (b:B {id:2}) \
             MERGE (a)-[r:REL {p:1}]-(b) RETURN count(r) AS count",
        )
        .expect("match undirected parallel edges");
    assert_eq!(
        undirected.batches[0]
            .column_by_name("count")
            .expect("count column")
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("integer count")
            .value(0),
        3
    );

    let error = forge.execute(
        "MATCH (a:A {id:1}), (b:B {id:2}) \
         MERGE (a)-[r:REL {p:null}]->(b) RETURN r",
    );
    assert!(
        error.is_err(),
        "null relationship MERGE values are rejected"
    );
    let after_error = forge
        .execute("MATCH ()-[r:REL]->() RETURN count(r) AS count")
        .expect("read relationships after rejected mutation");
    assert_eq!(
        after_error.batches[0]
            .column_by_name("count")
            .expect("count column")
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("integer count")
            .value(0),
        3
    );
}
