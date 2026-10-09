//! Public-facade acceptance pins for the exact workload Cypher constructs the
//! GDC benchmark runners currently rewrite around. Every test states the
//! openCypher semantics through the public [`GraphForge::execute`] /
//! [`GraphForge::execute_with_params`] boundary with literal expected rows: a
//! stored temporal property component read in a filter and an aggregation, a
//! parameterized duration constructor map, an `OPTIONAL MATCH` whose WHERE
//! references a `WITH`-bound value, node-list concatenation feeding `UNWIND`
//! and a pattern, a collected node list deduplicated by `WITH DISTINCT`
//! feeding `OPTIONAL MATCH`, `UNWIND` of list-of-map parameters across two
//! label schemas, an `ALL` quantifier indexing variable-length path
//! relationships, a path-node list comprehension filtered in the same `WITH`,
//! variable-length multi-type relationship alternation in both directions,
//! `startNode` / `endNode` and `id` over unwound path hops, `reduce` over
//! scalar lists and path-hop amounts, a `COUNT { … }` subquery, `CALL { … }`
//! subqueries (uncorrelated, correlated, `UNION ALL` inside the body), and
//! `shortestPath` bound in `MATCH` position.

use std::collections::HashMap;

use arrow::array::Array;
use arrow::util::display::array_value_to_string;
use graphforge_api::{GraphForge, IrLiteral};

/// Every column of every row, rendered; `None` for a null cell. Rows keep the
/// engine's order — deterministic queries carry their own `ORDER BY`.
fn rows_with(
    gf: &GraphForge,
    query: &str,
    params: &HashMap<String, IrLiteral>,
) -> Vec<Vec<Option<String>>> {
    let result = gf
        .execute_with_params(query, params)
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    let mut rows = Vec::new();
    for batch in &result.batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|array| {
                        (!array.is_null(row))
                            .then(|| array_value_to_string(array, row).expect("render"))
                    })
                    .collect(),
            );
        }
    }
    rows
}

fn rows(gf: &GraphForge, query: &str) -> Vec<Vec<Option<String>>> {
    rows_with(gf, query, &HashMap::new())
}

fn strings(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_owned())).collect()
}

/// `1 -transfer{amount: 10}-> 2 -transfer{amount: 20}-> 3 -withdraw{amount: 5}-> 4`.
fn accounts() -> GraphForge {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute(
        "CREATE (a1:Account {id: 1, name: 'a'}), (a2:Account {id: 2, name: 'b'}), \
         (a3:Account {id: 3, name: 'c'}), (a4:Account {id: 4, name: 'd'}), \
         (a1)-[:transfer {amount: 10}]->(a2), (a2)-[:transfer {amount: 20}]->(a3), \
         (a3)-[:withdraw {amount: 5}]->(a4)",
    )
    .expect("create accounts");
    gf
}

/// Two stored `creationDate` dates spanning two years.
fn messages() -> GraphForge {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute(
        "CREATE (:Msg {creationDate: date('2012-01-01')}), \
         (:Msg {creationDate: date('2013-01-01')})",
    )
    .expect("create messages");
    gf
}

/// Two label schemas with disjoint property sets: `L1` keys on `left_key`,
/// `L2` keys on `right_key`.
fn schemas() -> GraphForge {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:L1 {left_key: 7, title: 'left'}), (:L2 {right_key: 'k', enabled: true})")
        .expect("create schemas");
    gf
}

#[test]
fn temporal_component_of_stored_property_filters_and_aggregates() {
    let gf = messages();
    // Direct component read of a stored property — no alias workaround.
    assert_eq!(
        rows(
            &gf,
            "MATCH (n:Msg) WHERE n.creationDate.year = 2012 RETURN count(n)"
        ),
        vec![strings(&["1"])]
    );
    assert_eq!(
        rows(
            &gf,
            "MATCH (n:Msg) RETURN n.creationDate.year AS y, count(*) AS n ORDER BY y"
        ),
        vec![strings(&["2012", "1"]), strings(&["2013", "1"])]
    );
}

#[test]
fn parameterized_duration_hours_equals_seconds_and_bounds_arithmetic() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    for (delta, seconds) in [(0, 0), (1, 3_600), (-1, -3_600), (24, 86_400)] {
        let params = HashMap::from([
            ("delta".to_owned(), IrLiteral::Int(delta)),
            ("seconds".to_owned(), IrLiteral::Int(seconds)),
        ]);
        assert_eq!(
            rows_with(
                &gf,
                "RETURN duration({hours: $delta}) = duration({seconds: $seconds})",
                &params
            ),
            vec![strings(&["true"])],
            "{delta}h"
        );
    }
    // A whole-hour duration crossing a day boundary, literal and fixture
    // independent.
    assert_eq!(
        rows(
            &gf,
            "RETURN datetime('2012-01-01T23:00Z') + duration({hours: 2}) \
             = datetime('2012-01-02T01:00Z'), \
             date('2012-01-01') + duration({hours: 24}) = date('2012-01-02')"
        ),
        vec![strings(&["true", "true"])]
    );
}

#[test]
fn optional_match_where_compares_against_a_with_bound_cutoff() {
    let gf = accounts();
    assert_eq!(
        rows(
            &gf,
            "MATCH (a:Account {id: 1}) WITH a, 15 AS cutoff \
             OPTIONAL MATCH (a)-[r:transfer]->(b) WHERE r.amount > cutoff \
             RETURN a.id, b.id"
        ),
        vec![vec![Some("1".to_owned()), None]]
    );
    assert_eq!(
        rows(
            &gf,
            "MATCH (a:Account {id: 1}) WITH a, 5 AS cutoff \
             OPTIONAL MATCH (a)-[r:transfer]->(b) WHERE r.amount > cutoff \
             RETURN a.id, b.id"
        ),
        vec![strings(&["1", "2"])]
    );
}

#[test]
fn concatenated_node_lists_unwind_into_a_pattern_match() {
    let gf = accounts();
    // The duplicate `a` element survives: each list entry matches its own
    // outgoing transfer.
    assert_eq!(
        rows(
            &gf,
            "MATCH (a:Account {id: 1}), (b:Account {id: 2}) \
             WITH [a, a] + [b] AS ns UNWIND ns AS n \
             MATCH (n)-[:transfer]->(m) \
             RETURN n.id AS source, m.id AS dest ORDER BY source, dest"
        ),
        vec![
            strings(&["1", "2"]),
            strings(&["1", "2"]),
            strings(&["2", "3"]),
        ]
    );
}

/// The collected-list shape: a `collect`-built node list grown by
/// concatenation and deduplicated by `WITH DISTINCT` feeds `OPTIONAL MATCH`
/// directly, with no fresh required `MATCH` after the `UNWIND` that could
/// rebind the element.
#[test]
fn collected_node_list_with_distinct_feeds_optional_match() {
    let gf = accounts();
    assert_eq!(
        rows(
            &gf,
            "MATCH (a:Account {id: 1}) WITH collect(a) AS xs \
             MATCH (b:Account {id: 2}) WITH xs, xs + collect(b) AS ns \
             UNWIND ns AS n WITH DISTINCT n \
             OPTIONAL MATCH (n)-[:transfer]->(m) \
             RETURN n.id AS source, m.id AS dest ORDER BY source, dest"
        ),
        vec![strings(&["1", "2"]), strings(&["2", "3"])]
    );
}

#[test]
fn unwound_map_parameter_rows_match_across_label_schemas() {
    let gf = schemas();
    let row = || {
        IrLiteral::Map(vec![
            ("left_key".to_owned(), IrLiteral::Int(7)),
            ("right_key".to_owned(), IrLiteral::Str("k".to_owned())),
        ])
    };
    let params = HashMap::from([(
        "rows".to_owned(),
        IrLiteral::List(vec![
            row(),
            row(),
            IrLiteral::Map(vec![
                ("left_key".to_owned(), IrLiteral::Int(99)),
                ("right_key".to_owned(), IrLiteral::Str("missing".to_owned())),
            ]),
        ]),
    )]);
    // The duplicate parameter row is admitted twice; the 99/'missing' row
    // matches nothing and contributes no rows.
    assert_eq!(
        rows_with(
            &gf,
            "UNWIND $rows AS row \
             MATCH (a:L1 {left_key: row.left_key}), (b:L2 {right_key: row.right_key}) \
             RETURN a.title AS t, b.enabled AS e ORDER BY t, e",
            &params
        ),
        vec![strings(&["left", "true"]), strings(&["left", "true"])]
    );
}

#[test]
fn all_quantifier_indexes_variable_length_relationship_properties() {
    let gf = accounts();
    assert_eq!(
        rows(
            &gf,
            "MATCH (a:Account {id: 1}) MATCH (a)-[rs:transfer*1..3]->(b) \
             WHERE ALL(i IN range(0, size(rs) - 1) WHERE rs[i].amount > 0) \
             RETURN b.id AS dest, size(rs) AS hops ORDER BY dest, hops"
        ),
        vec![strings(&["2", "1"]), strings(&["3", "2"])]
    );
}

#[test]
fn path_node_list_comprehension_filters_in_the_same_with() {
    let gf = accounts();
    assert_eq!(
        rows(
            &gf,
            "MATCH (a:Account {id: 1}) MATCH p = (a)-[:transfer*1..3]->(b) \
             WITH b, [n IN nodes(p) | n.id] AS ids WHERE size(ids) > 2 \
             RETURN b.id AS dest, size(ids) AS seen"
        ),
        vec![strings(&["3", "3"])]
    );
}

#[test]
fn variable_length_multi_type_alternation_traverses_both_directions() {
    let gf = accounts();
    assert_eq!(
        rows(
            &gf,
            "MATCH (a:Account {id: 1}) MATCH (a)-[rs:transfer|withdraw*1..3]->(b) \
             RETURN b.id AS dest, size(rs) AS hops ORDER BY dest, hops"
        ),
        vec![
            strings(&["2", "1"]),
            strings(&["3", "2"]),
            strings(&["4", "3"]),
        ]
    );
    // Distinct real paths are not collapsed, and reversal walks against the
    // stored orientation.
    assert_eq!(
        rows(
            &gf,
            "MATCH (a:Account {id: 4}) MATCH (a)<-[rs:transfer|withdraw*1..3]-(b) \
             RETURN b.id AS dest, size(rs) AS hops ORDER BY dest, hops"
        ),
        vec![
            strings(&["1", "3"]),
            strings(&["2", "2"]),
            strings(&["3", "1"]),
        ]
    );
}

#[test]
fn start_and_end_node_of_each_unwound_variable_path_hop() {
    let gf = accounts();
    assert_eq!(
        rows(
            &gf,
            "MATCH (a:Account {id: 1}) MATCH p = (a)-[:transfer*2..2]->(b) \
             UNWIND relationships(p) AS hop \
             RETURN startNode(hop).id AS s, endNode(hop).id AS e ORDER BY s, e"
        ),
        vec![strings(&["1", "2"]), strings(&["2", "3"])]
    );
}

#[test]
fn relationship_identity_of_variable_path_hops_matches_a_fixed_edge() {
    let gf = accounts();
    let prefix = "MATCH (a:Account {id: 1}), (x:Account {id: 1}) \
                  MATCH p = (a)-[:transfer*2..2]->(b) \
                  UNWIND relationships(p) AS hop \
                  MATCH (x)-[f:transfer]->(:Account {id: 2}) ";
    // Two hops, each with a non-null identity, all distinct: the path binds
    // two different relationships without collapsing them.
    assert_eq!(
        rows(
            &gf,
            &format!(
                "{prefix}RETURN count(*) AS pairs, count(id(hop)) AS identified, \
                 count(DISTINCT id(hop)) AS distinct_ids"
            )
        ),
        vec![strings(&["2", "2", "2"])]
    );
    // Identity equality holds across bindings: exactly one hop is the fixed
    // 1 ->transfer 2 edge, without asserting on the rendered identity.
    assert_eq!(
        rows(
            &gf,
            &format!("{prefix}WHERE id(hop) = id(f) RETURN count(*) AS matched")
        ),
        vec![strings(&["1"])]
    );
}

#[test]
fn reduce_folds_scalar_lists_and_path_hop_amounts() {
    let gf = accounts();
    assert_eq!(
        rows(&gf, "RETURN reduce(total = 0, x IN [1, 2, 3] | total + x)"),
        vec![strings(&["6"])]
    );
    assert_eq!(
        rows(&gf, "RETURN reduce(total = 0, x IN [] | total + x)"),
        vec![strings(&["0"])]
    );
    assert_eq!(
        rows(
            &gf,
            "MATCH (a:Account {id: 1}) MATCH p = (a)-[:transfer*1..2]->(b) \
             RETURN b.id AS dest, \
             reduce(total = 0, hop IN relationships(p) | total + hop.amount) AS total \
             ORDER BY dest"
        ),
        vec![strings(&["2", "10"]), strings(&["3", "30"])]
    );
}

#[test]
fn count_subquery_counts_per_outer_row() {
    let gf = accounts();
    assert_eq!(
        rows(
            &gf,
            "MATCH (a:Account) RETURN a.id AS dest, COUNT { (a)-[:transfer]->() } AS n \
             ORDER BY dest"
        ),
        vec![
            strings(&["1", "1"]),
            strings(&["2", "1"]),
            strings(&["3", "0"]),
            strings(&["4", "0"]),
        ]
    );
}

#[test]
fn uncorrelated_call_subquery_returns_its_own_rows() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    assert_eq!(
        rows(&gf, "CALL { RETURN 1 AS value } RETURN value"),
        vec![strings(&["1"])]
    );
}

#[test]
fn correlated_call_subquery_sees_the_outer_variable() {
    let gf = accounts();
    assert_eq!(
        rows(
            &gf,
            "MATCH (a:Account) CALL { WITH a MATCH (a)-[:transfer]->(b) RETURN b.id AS target } \
             RETURN a.id AS source, target ORDER BY source, target"
        ),
        vec![strings(&["1", "2"]), strings(&["2", "3"])]
    );
}

#[test]
fn call_subqueries_compose_with_union_all() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    assert_eq!(
        rows(
            &gf,
            "CALL { RETURN 1 AS value UNION ALL RETURN 1 AS value } RETURN value"
        ),
        vec![strings(&["1"]), strings(&["1"])]
    );
}

/// Official `MATCH`-position binding; hop counts come from `length(p)`
/// (`size` on a Path is a type error in openCypher). Each fixture pair has a
/// unique shortest path, so no `ORDER BY` is needed.
#[test]
fn shortest_path_returns_the_minimum_hop_count() {
    let gf = accounts();
    assert_eq!(
        rows(
            &gf,
            "MATCH (a:Account {id: 1}), (b:Account {id: 3}) \
             MATCH p = shortestPath((a)-[:transfer*]->(b)) \
             RETURN length(p)"
        ),
        vec![strings(&["2"])]
    );
    assert_eq!(
        rows(
            &gf,
            "MATCH (a:Account {id: 1}), (b:Account {id: 4}) \
             MATCH p = shortestPath((a)-[:transfer|withdraw*]->(b)) \
             RETURN length(p)"
        ),
        vec![strings(&["3"])]
    );
}

/// A bounded `*1..1` bound over the single direct edge.
#[test]
fn shortest_path_bounded_to_one_hop_returns_the_single_edge() {
    let gf = accounts();
    assert_eq!(
        rows(
            &gf,
            "MATCH (a:Account {id: 1}), (b:Account {id: 2}) \
             MATCH p = shortestPath((a)-[:transfer*1..1]->(b)) \
             RETURN length(p)"
        ),
        vec![strings(&["1"])]
    );
}
