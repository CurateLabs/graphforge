//! Public-facade acceptance pins for the exact workload Cypher constructs the
//! GDC benchmark runners currently rewrite around. Every test states the
//! openCypher semantics or a specific typed unsupported-feature refusal through the public [`GraphForge::execute`] /
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
//! `shortestPath` bound in `MATCH` position, proved node-list facts carrying
//! `collect`ed (and concatenated, renamed, deduplicated) node lists through
//! `UNWIND` into `OPTIONAL MATCH` with duplicates and null extensions
//! preserved, and the element kinds that must stay value-typed at a later node
//! pattern.

use std::collections::HashMap;

use arrow::array::Array;
use arrow::util::display::array_value_to_string;
use graphforge_api::{GfError, GraphForge, IrLiteral};

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

fn unsupported(gf: &GraphForge, query: &str, params: &HashMap<String, IrLiteral>, feature: &str) {
    let error = gf
        .execute_with_params(query, params)
        .expect_err("specific unsupported construct");
    assert_eq!(error.code(), "GF_NOT_IMPLEMENTED", "{query}: {error}");
    assert!(
        matches!(&error, GfError::NotImplemented(actual) if actual == feature),
        "{query}: {error:?}"
    );
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
fn dynamic_duration_maps_are_explicitly_unsupported() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    for (delta, seconds) in [(0, 0), (1, 3_600), (-1, -3_600), (24, 86_400)] {
        let params = HashMap::from([
            ("delta".to_owned(), IrLiteral::Int(delta)),
            ("seconds".to_owned(), IrLiteral::Int(seconds)),
        ]);
        unsupported(
            &gf,
            "RETURN duration({hours: $delta}) = duration({seconds: $seconds})",
            &params,
            "Cypher duration maps with nonliteral values",
        );
    }
    // Literal constructors and their calendar arithmetic retain supported semantics.
    assert_eq!(
        rows(
            &gf,
            "RETURN datetime('2012-01-01T23:00Z') + duration({hours: 2}) = datetime('2012-01-02T01:00Z'), date('2012-01-01') + duration({hours: 24}) = date('2012-01-02')"
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
fn parameter_row_matches_across_labels_are_explicitly_unsupported() {
    let gf = schemas();
    let row = || {
        IrLiteral::Map(vec![
            ("left_key".to_owned(), IrLiteral::Int(7)),
            ("right_key".to_owned(), IrLiteral::Str("k".to_owned())),
        ])
    };
    let params = HashMap::from([("rows".to_owned(), IrLiteral::List(vec![row(), row()]))]);
    unsupported(
        &gf,
        "UNWIND $rows AS row MATCH (a:L1 {left_key: row.left_key}), (b:L2 {right_key: row.right_key}) RETURN a.title AS t, b.enabled AS e ORDER BY t, e",
        &params,
        "Cypher parameter rows matching properties across different node labels",
    );
}

#[test]
fn indexed_variable_relationship_all_is_explicitly_unsupported() {
    let gf = accounts();
    unsupported(
        &gf,
        "MATCH (a:Account {id: 1}) MATCH (a)-[rs:transfer*1..3]->(b) WHERE ALL(i IN range(0, size(rs) - 1) WHERE rs[i].amount > 0) RETURN b.id AS dest, size(rs) AS hops ORDER BY dest, hops",
        &HashMap::new(),
        "Cypher ALL predicates over indexed variable-length relationship properties",
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
fn variable_length_type_alternation_is_explicitly_unsupported() {
    let gf = accounts();
    unsupported(
        &gf,
        "MATCH (a:Account {id: 1}) MATCH (a)-[rs:transfer|withdraw*1..3]->(b) RETURN b.id AS dest, size(rs) AS hops ORDER BY dest, hops",
        &HashMap::new(),
        "Cypher variable-length relationship type alternation",
    );
    unsupported(
        &gf,
        "MATCH (a:Account {id: 4}) MATCH (a)<-[rs:transfer|withdraw*1..3]-(b) RETURN b.id AS dest, size(rs) AS hops ORDER BY dest, hops",
        &HashMap::new(),
        "Cypher variable-length relationship type alternation",
    );
}

#[test]
fn endpoints_of_unwound_path_hops_are_explicitly_unsupported() {
    let gf = accounts();
    unsupported(
        &gf,
        "MATCH (a:Account {id: 1}) MATCH p = (a)-[:transfer*2..2]->(b) UNWIND relationships(p) AS hop RETURN startNode(hop).id AS s, endNode(hop).id AS e ORDER BY s, e",
        &HashMap::new(),
        "Cypher endpoint functions on unbound relationship values",
    );
}

#[test]
fn id_function_is_explicitly_unsupported() {
    let gf = accounts();
    unsupported(
        &gf,
        "MATCH (a:Account {id: 1}), (x:Account {id: 1}) MATCH p = (a)-[:transfer*2..2]->(b) UNWIND relationships(p) AS hop MATCH (x)-[f:transfer]->(:Account {id: 2}) RETURN count(*) AS pairs, count(id(hop)) AS identified, count(DISTINCT id(hop)) AS distinct_ids",
        &HashMap::new(),
        "Cypher id identity function",
    );
    unsupported(
        &gf,
        "MATCH (a:Account {id: 1}), (x:Account {id: 1}) MATCH p = (a)-[:transfer*2..2]->(b) UNWIND relationships(p) AS hop MATCH (x)-[f:transfer]->(:Account {id: 2}) WHERE id(hop) = id(f) RETURN count(*) AS matched",
        &HashMap::new(),
        "Cypher id identity function",
    );
}

#[test]
fn reduce_expressions_are_explicitly_unsupported() {
    let gf = accounts();
    unsupported(
        &gf,
        "RETURN reduce(total = 0, x IN [1, 2, 3] | total + x)",
        &HashMap::new(),
        "Cypher reduce accumulator expressions",
    );
    unsupported(
        &gf,
        "RETURN reduce(total = 0, x IN [] | total + x)",
        &HashMap::new(),
        "Cypher reduce accumulator expressions",
    );
    unsupported(
        &gf,
        "MATCH (a:Account {id: 1}) MATCH p = (a)-[:transfer*1..2]->(b) RETURN b.id AS dest, reduce(total = 0, hop IN relationships(p) | total + hop.amount) AS total ORDER BY dest",
        &HashMap::new(),
        "Cypher reduce accumulator expressions",
    );
}

#[test]
fn count_subqueries_are_explicitly_unsupported() {
    let gf = accounts();
    unsupported(
        &gf,
        "MATCH (a:Account) RETURN a.id AS dest, COUNT { (a)-[:transfer]->() } AS n ORDER BY dest",
        &HashMap::new(),
        "Cypher COUNT subqueries",
    );
}

#[test]
fn uncorrelated_call_subqueries_are_explicitly_unsupported() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    unsupported(
        &gf,
        "CALL { RETURN 1 AS value } RETURN value",
        &HashMap::new(),
        "Cypher CALL subqueries",
    );
}

#[test]
fn correlated_call_subqueries_are_explicitly_unsupported() {
    let gf = accounts();
    unsupported(
        &gf,
        "MATCH (a:Account) CALL { WITH a MATCH (a)-[:transfer]->(b) RETURN b.id AS target } RETURN a.id AS source, target ORDER BY source, target",
        &HashMap::new(),
        "Cypher CALL subqueries",
    );
}

#[test]
fn union_call_subqueries_are_explicitly_unsupported() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    unsupported(
        &gf,
        "CALL { RETURN 1 AS value UNION ALL RETURN 1 AS value } RETURN value",
        &HashMap::new(),
        "Cypher CALL subqueries",
    );
}

/// Official `MATCH`-position binding; hop counts come from `length(p)`
/// (`size` on a Path is a type error in openCypher). Each fixture pair has a
/// unique shortest path, so no `ORDER BY` is needed.
#[test]
fn shortest_path_patterns_are_explicitly_unsupported() {
    let gf = accounts();
    unsupported(
        &gf,
        "MATCH (a:Account {id: 1}), (b:Account {id: 3}) MATCH p = shortestPath((a)-[:transfer*]->(b)) RETURN length(p)",
        &HashMap::new(),
        "Cypher shortest-path patterns",
    );
    unsupported(
        &gf,
        "MATCH (a:Account {id: 1}), (b:Account {id: 4}) MATCH p = shortestPath((a)-[:transfer|withdraw*]->(b)) RETURN length(p)",
        &HashMap::new(),
        "Cypher shortest-path patterns",
    );
}

/// A bounded `*1..1` bound over the single direct edge.
#[test]
fn bounded_shortest_path_patterns_are_explicitly_unsupported() {
    let gf = accounts();
    unsupported(
        &gf,
        "MATCH (a:Account {id: 1}), (b:Account {id: 2}) MATCH p = shortestPath((a)-[:transfer*1..1]->(b)) RETURN length(p)",
        &HashMap::new(),
        "Cypher shortest-path patterns",
    );
}

/// Duplicate elements of a proved node list survive `UNWIND` + `OPTIONAL
/// MATCH` (the repeated `1` matches its transfer twice), and an element with no
/// outgoing `transfer` extends the result with a null column instead of
/// dropping the row.
#[test]
fn collected_duplicates_and_null_extension_survive_unwind_optional_match() {
    let gf = accounts();
    assert_eq!(
        rows(
            &gf,
            "MATCH (a:Account {id: 1}) WITH collect(a) AS xs \
             MATCH (b:Account {id: 4}) WITH xs, xs + xs + collect(b) AS ns \
             UNWIND ns AS n \
             OPTIONAL MATCH (n)-[:transfer]->(m) \
             RETURN n.id AS source, m.id AS dest ORDER BY source, dest"
        ),
        vec![
            strings(&["1", "2"]),
            strings(&["1", "2"]),
            vec![Some("4".to_owned()), None],
        ]
    );
}

/// Proved node-list facts follow renamed aliases across plain WITH projections
/// (`collect(b) AS ys`, then `ys AS zs`) before the `UNWIND`; the renamed lists
/// concatenate, `WITH DISTINCT` deduplicates, and `OPTIONAL MATCH` still sees
/// whole nodes.
#[test]
fn node_list_facts_follow_renamed_aliases_through_multiple_withs() {
    let gf = accounts();
    assert_eq!(
        rows(
            &gf,
            "MATCH (a:Account {id: 1}) WITH collect(a) AS xs \
             MATCH (b:Account {id: 2}) WITH xs, collect(b) AS ys \
             WITH ys AS zs, xs AS ws \
             UNWIND ws + zs AS n \
             WITH DISTINCT n \
             OPTIONAL MATCH (n)-[:transfer]->(m) \
             RETURN n.id AS source, m.id AS dest ORDER BY source, dest"
        ),
        vec![strings(&["1", "2"]), strings(&["2", "3"])]
    );
}

/// Mixed owner labels keep the proved node kind but drop the shared label — the
/// unwound elements still match patterns by identity, and an element with no
/// outgoing edge extends the result with a null column.
#[test]
fn mixed_label_node_lists_keep_the_node_kind_without_an_owner_label() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:A {id: 1})-[:r]->(:B {id: 2})")
        .expect("create mixed labels");
    assert_eq!(
        rows(
            &gf,
            "MATCH (a:A {id: 1}) WITH collect(a) AS xs \
             MATCH (b:B {id: 2}) WITH xs, xs + collect(b) AS ns \
             UNWIND ns AS n \
             OPTIONAL MATCH (n)-[:r]->(m) \
             RETURN n.id AS source, m.id AS dest ORDER BY source, dest"
        ),
        vec![strings(&["1", "2"]), vec![Some("2".to_owned()), None]]
    );
}

/// Element kinds that prove nothing stay runtime values: `UNWIND` over them
/// keeps the binder's value-kind conflict at a later node pattern instead of
/// silently promoting the elements to nodes.
#[test]
fn unproved_element_kinds_keep_the_value_kind_conflict_at_a_node_pattern() {
    let gf = accounts();
    let conflict = |query: &str| {
        let error = gf
            .execute(query)
            .expect_err("unproved elements must stay value-typed");
        assert!(
            error.to_string().contains("bound as a value"),
            "{query}: {error}"
        );
    };
    // Scalar list.
    conflict(
        "MATCH (a:Account {id: 1}) WITH [1, 2] AS ns UNWIND ns AS n \
         WITH DISTINCT n OPTIONAL MATCH (n)-[:transfer]->(m) RETURN n.id",
    );
    // List of relationships.
    conflict(
        "MATCH ()-[r:transfer]->() WITH [r] AS rs UNWIND rs AS n \
         WITH DISTINCT n OPTIONAL MATCH (n)-[:transfer]->(m) RETURN n.id",
    );
    // Nested list of nodes.
    conflict(
        "MATCH (a:Account {id: 1}) WITH [[a]] AS ns UNWIND ns AS n \
         WITH DISTINCT n OPTIONAL MATCH (n)-[:transfer]->(m) RETURN n.id",
    );
    // List of maps.
    conflict(
        "WITH [{k: 1}] AS ns UNWIND ns AS n \
         WITH DISTINCT n OPTIONAL MATCH (n)-[:transfer]->(m) RETURN n.id",
    );
    // Unknown parameter element type.
    let error = gf
        .execute_with_params(
            "UNWIND $list AS n WITH DISTINCT n \
             OPTIONAL MATCH (n)-[:transfer]->(m) RETURN n.id",
            &HashMap::from([("list".to_owned(), IrLiteral::List(vec![]))]),
        )
        .expect_err("parameter element types prove nothing");
    assert!(error.to_string().contains("bound as a value"), "{error}");
}

#[test]
fn supported_fixed_relationship_endpoints_and_scalar_all_keep_their_semantics() {
    let gf = accounts();
    assert_eq!(
        rows(
            &gf,
            "MATCH ()-[r]->() RETURN startNode(r).id AS s, endNode(r).id AS e ORDER BY s"
        ),
        vec![
            strings(&["1", "2"]),
            strings(&["2", "3"]),
            strings(&["3", "4"])
        ]
    );
    assert_eq!(
        rows(&gf, "RETURN ALL(x IN [1, 2, 3] WHERE x > 0)"),
        vec![strings(&["true"])]
    );
}

#[test]
fn malformed_unsupported_forms_keep_syntax_errors() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    for query in [
        "RETURN reduce(total = 0, x IN [1] total + x)",
        "RETURN COUNT { (a)-[:r]-> }",
        "MATCH p = shortestPath((a)-[:r*]->) RETURN p",
        "CALL { RETURN } RETURN 1",
        "CALL { RETURN 1 AS n } RETURN",
        "RETURN reduce(total = 0, x IN [1] | total + x) + )",
        "RETURN COUNT { RETURN 1 AS n } + )",
        "CALL { RETURN reduce(total = 0, x IN [1] | total + x)",
        "MATCH p = shortestPath((a)-[:r*]->(b)) RETURN",
    ] {
        assert!(
            matches!(gf.execute(query), Err(GfError::Parse { .. })),
            "{query}"
        );
    }
}

/// UNWIND introduces a fresh name, including when the existing name is a
/// previously unwound node. WITH may remove that name before a later UNWIND.
#[test]
fn unwind_rejects_an_alias_already_in_scope() {
    let gf = accounts();
    for query in [
        "UNWIND [1] AS n UNWIND [2] AS n RETURN n",
        "MATCH (a:Account {id: 1}) WITH collect(a) AS xs UNWIND xs AS n UNWIND [n] AS n RETURN n.id",
        "MATCH (a:Account {id: 1}) WITH collect(a) AS xs UNWIND xs AS n UNWIND [1] AS n WITH collect(n) AS ns UNWIND ns AS x OPTIONAL MATCH (x)-[:transfer]->(m) RETURN x.id",
    ] {
        let error = gf
            .execute(query)
            .expect_err("UNWIND cannot overwrite an in-scope alias");
        assert_eq!(error.code(), "GF_PARSE", "{query}: {error}");
        assert!(
            error.to_string().contains("VariableAlreadyBound"),
            "{query}: {error}"
        );
    }
}

#[test]
fn unwound_node_recollection_preserves_properties_and_identity() {
    let gf = accounts();
    for collection in ["collect(n)", "collect(DISTINCT n)"] {
        let query = format!(
            "MATCH (a:Account {{id: 1}}) WITH collect(a) AS xs \
            UNWIND xs AS n WITH {collection} AS ns UNWIND ns AS n \
            OPTIONAL MATCH (n)-[:transfer]->(m) \
            RETURN n.id AS source, m.id AS dest"
        );
        assert_eq!(rows(&gf, &query), vec![strings(&["1", "2"])], "{query}");
    }
    // Graph-pattern enrichment adds topology without replacing value fields.
    assert_eq!(
        rows(
            &gf,
            "MATCH (a:Account {id: 1}) WITH collect(a) AS xs \
        UNWIND xs AS n MATCH (n)-[:transfer]->(m) \
        WITH collect(n) AS ns UNWIND ns AS x RETURN x.id AS source"
        ),
        vec![strings(&["1"])]
    );
}

#[test]
fn unsupported_features_preserve_ordinary_binding_errors() {
    let gf = schemas();
    let params = HashMap::from([(
        "rows".to_owned(),
        IrLiteral::List(vec![IrLiteral::Map(vec![
            ("left_key".to_owned(), IrLiteral::Int(7)),
            ("right_key".to_owned(), IrLiteral::Str("k".to_owned())),
        ])]),
    )]);
    for query in [
        "RETURN id(missing)",
        "RETURN duration({hours: missing})",
        "UNWIND $rows AS row MATCH (a:L1 {left_key: row.left_key}), (b:L2 {right_key: row.right_key}) RETURN missing",
    ] {
        let error = gf
            .execute_with_params(query, &params)
            .expect_err("undefined name");
        assert_eq!(error.code(), "GF_PARSE", "{query}: {error}");
        let GfError::Bind { diagnostics, .. } = error else {
            panic!("{query}: {error}")
        };
        assert!(
            diagnostics
                .iter()
                .any(|diagnostic| diagnostic.kind
                    == graphforge_core::BindErrorKind::UndeclaredVariable),
            "{query}: {diagnostics:?}"
        );
    }
}

#[test]
fn parameter_row_refusal_tracks_with_wildcards_and_aliases() {
    let gf = schemas();
    let params = HashMap::from([(
        "rows".to_owned(),
        IrLiteral::List(vec![IrLiteral::Map(vec![
            ("left_key".to_owned(), IrLiteral::Int(7)),
            ("right_key".to_owned(), IrLiteral::Str("k".to_owned())),
        ])]),
    )]);
    for query in [
        "UNWIND $rows AS row MATCH (a:L1 {left_key: row.left_key}) WITH * MATCH (b:L2 {right_key: row.right_key}) RETURN a.title, b.enabled",
        "UNWIND $rows AS row MATCH (a:L1 {left_key: row.left_key}) WITH a, row AS next MATCH (b:L2 {right_key: next.right_key}) RETURN a.title, b.enabled",
    ] {
        unsupported(
            &gf,
            query,
            &params,
            "Cypher parameter rows matching properties across different node labels",
        );
    }
}

#[test]
fn all_predicate_local_alias_shadows_outer_relationship_list() {
    let gf = accounts();
    assert_eq!(
        rows(
            &gf,
            "MATCH (:Account {id: 1})-[r:transfer*1..2]->(:Account) \
        RETURN ALL(r IN [[{amount: 1}]] WHERE r[0].amount = 1) AS ok"
        ),
        vec![strings(&["true"]), strings(&["true"])]
    );
    assert_eq!(
        rows(
            &gf,
            "MATCH (:Account {id: 1})-[r:transfer*1..2]->(:Account) \
        RETURN ALL(x IN [1] WHERE ANY(r IN [[{amount: 1}]] WHERE r[0].amount = x)) AS ok"
        ),
        vec![strings(&["true"]), strings(&["true"])]
    );
}
