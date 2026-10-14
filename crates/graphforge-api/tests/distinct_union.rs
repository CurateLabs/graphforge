//! Persistent-project regression coverage for distinct UNION over properties.

use arrow::util::display::array_value_to_string;
use graphforge_api::GraphForge;

const DISTINCT_QUERIES: [(&str, &str, &[&str]); 4] = [
    (
        "MATCH (n:P) RETURN n.id AS id UNION MATCH (m:Q) RETURN m.id AS id",
        "MATCH (n:P) RETURN n.id AS id UNION ALL MATCH (m:Q) RETURN m.id AS id",
        &["1", "2", "3"],
    ),
    (
        "MATCH (n:P) RETURN n.name AS x UNION MATCH (n:Q) RETURN n.name AS x",
        "MATCH (n:P) RETURN n.name AS x UNION ALL MATCH (n:Q) RETURN n.name AS x",
        &["a", "a", "b"],
    ),
    (
        "MATCH (n:P) RETURN n.id AS x UNION RETURN 1 AS x",
        "MATCH (n:P) RETURN n.id AS x UNION ALL RETURN 1 AS x",
        &["1", "1", "2"],
    ),
    (
        "MATCH ()-[r:R]->() RETURN r.w AS w UNION MATCH ()-[s:S]->() RETURN s.w AS w",
        "MATCH ()-[r:R]->() RETURN r.w AS w UNION ALL MATCH ()-[s:S]->() RETURN s.w AS w",
        &["1", "1", "2"],
    ),
];

fn rows(graph: &GraphForge, query: &str) -> Vec<String> {
    let result = graph.execute(query).unwrap_or_else(|error| {
        panic!("query failed: {query}: {error}");
    });
    let mut rows = result
        .batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows())
                .map(|row| array_value_to_string(batch.column(0).as_ref(), row).unwrap())
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

fn assert_queries(graph: &GraphForge) {
    for (distinct, all, expected_all) in DISTINCT_QUERIES {
        let all_rows = rows(graph, all);
        assert_eq!(
            all_rows,
            expected_all
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        );
        let distinct_rows = rows(graph, distinct);
        let mut deduplicated = all_rows;
        deduplicated.dedup();
        assert_eq!(distinct_rows, deduplicated, "distinct query: {distinct}");
    }
    assert_eq!(
        rows(graph, DISTINCT_QUERIES[1].1).len(),
        3,
        "UNION ALL preserves both copies of 'a'"
    );
}

#[test]
fn distinct_union_over_persisted_properties_matches_union_all_deduplicated() {
    // Covers #2027: the query-facing schema must not carry storage live-schema metadata.
    let directory = tempfile::tempdir().expect("ext4 TMPDIR project directory");
    let path = directory.path().to_str().expect("UTF-8 project path");
    {
        let graph = GraphForge::new(Some(path)).expect("persistent graph");
        graph
            .execute(
                "CREATE (:P {id: 1, name: 'a'}), (:P {id: 2, name: 'b'}), (:Q {id: 3, name: 'a'})",
            )
            .expect("commit fixture nodes");
        graph
            .execute(
                "CREATE (:E)-[:R {w: 1}]->(:F), (:E)-[:S {w: 1}]->(:F), (:E)-[:S {w: 2}]->(:F)",
            )
            .expect("commit fixture relationships");
        assert_queries(&graph);
    }
    let reopened = GraphForge::new(Some(path)).expect("reopen persistent graph");
    assert_queries(&reopened);
}
